//! Beacon API client.
//!
//! Fetches beacon blocks as SSZ and decodes them using the fork named by the
//! `Eth-Consensus-Version` response header. Responses are treated as untrusted:
//! status, headers, and body are all validated before use.

use std::time::{Duration, Instant};

use ::metrics::histogram;
use anyhow::{Context, Result, anyhow, bail};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use lighthouse_types::{
    Config, ForkName, ForkVersionDecode, Hash256, MainnetEthSpec, SignedBeaconBlock,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest_eventsource::{Event as SseEvent, EventSource};
use serde::Deserialize;
use url::Url;

use crate::config::BlockId;
use crate::metrics::REQUEST_STAGE_DURATION;

/// Bound on establishing a TCP connection to the Beacon API.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on each socket read. This is the hang detector for both unary calls
/// and the long-lived SSE event stream. The Beacon API event stream carries no
/// keep-alive comments — with `topics=block` an event arrives roughly every
/// slot (12s) — so 120s (~10 empty slots) separates a dead connection from a
/// quiet chain. A *total* request timeout would instead kill the healthy
/// event stream on schedule (it did, every 30s), dropping the blocks that
/// land in each reconnect gap.
///
/// Known residual gap (accepted): a chain that goes eventless for longer than
/// this — devnet lulls, non-finality stretches of empty slots — tears down a
/// healthy stream on schedule, and any block landing inside the reconnect
/// window is never requested: there is no backfill on reconnect, the stream
/// only carries blocks arriving after it opens. Backfill-on-reconnect is the
/// eventual fix if missed blocks matter on such chains.
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// Header carrying the consensus fork used to encode an SSZ beacon block.
const CONSENSUS_VERSION_HEADER: &str = "Eth-Consensus-Version";

/// A beacon block fetched from the Beacon API, tagged with its decoded fork.
#[derive(Debug)]
pub struct FetchedBlock {
    fork: ForkName,
    block: SignedBeaconBlock<MainnetEthSpec>,
}

impl FetchedBlock {
    /// The consensus fork the block was decoded as.
    pub fn fork(&self) -> ForkName {
        self.fork
    }

    /// The block's slot.
    pub fn slot(&self) -> u64 {
        self.block.slot().as_u64()
    }

    /// The block root (`hash_tree_root` of the beacon block message).
    pub fn root(&self) -> Hash256 {
        self.block.canonical_root()
    }

    /// The decoded signed beacon block.
    pub fn block(&self) -> &SignedBeaconBlock<MainnetEthSpec> {
        &self.block
    }
}

/// A `block` event from the Beacon API event stream.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockEvent {
    /// Slot of the new block.
    #[serde(deserialize_with = "deserialize_quoted_u64")]
    pub slot: u64,
    /// Root of the new block.
    pub block: Hash256,
    /// Whether the node is optimistically synced (the block is not yet verified).
    #[serde(default)]
    pub execution_optimistic: bool,
}

/// Deserializes a JSON-quoted integer (the Beacon API encodes slots as strings).
fn deserialize_quoted_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    raw.parse().map_err(serde::de::Error::custom)
}

/// Response body of `GET /eth/v1/beacon/genesis`.
#[derive(Debug, Clone, Deserialize)]
struct GenesisData {
    /// Unix timestamp of the chain's genesis.
    #[serde(deserialize_with = "deserialize_quoted_u64")]
    genesis_time: u64,
}

/// HTTP client for the Beacon API.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    endpoint: Url,
}

/// Parses "Name: Value" header strings into a [`HeaderMap`], ignoring blank
/// entries. An unset `PROOFESSOOR_BEACON_HEADER` is represented by one empty
/// string.
fn build_header_map(headers: &[String]) -> Result<HeaderMap> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for raw in headers {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Never echo the raw entry here: with the colon missing there is no
        // way to tell name from value, and the value may be a secret (API key).
        let (name, value) = trimmed.split_once(':').context(
            "invalid beacon header: expected 'Name: Value' (content redacted; it may contain a secret)",
        )?;
        let name: HeaderName = name
            .trim()
            .parse()
            .with_context(|| format!("invalid beacon header name '{}'", name.trim()))?;
        // The value may be a secret (API key), so never echo it back in the error.
        let value: HeaderValue = value
            .trim()
            .parse()
            .context("invalid beacon header value")?;
        map.insert(name, value);
    }
    Ok(map)
}

impl Client {
    /// Creates a client targeting the given Beacon API base URL, sending the
    /// given "Name: Value" headers (e.g. an API key) on every request. Blank
    /// entries are ignored, so an unset PROOFESSOOR_BEACON_HEADER is fine.
    pub fn new(endpoint: Url, headers: &[String]) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .default_headers(build_header_map(headers)?)
            .build()
            .context("failed to build Beacon API HTTP client")?;
        Ok(Self { http, endpoint })
    }

    /// Fetches a beacon block by identifier (`GET /eth/v2/beacon/blocks/{block_id}`).
    pub async fn get_block(&self, block_id: &BlockId) -> Result<FetchedBlock> {
        let path = format!("/eth/v2/beacon/blocks/{}", block_id.to_path_segment());
        let url = self
            .endpoint
            .join(&path)
            .context("failed to construct beacon block URL")?;

        let fetch_start = Instant::now();
        let response = self
            .http
            .get(url)
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .context("failed to reach the Beacon API")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("Beacon API returned {status} for block {block_id}: {body}");
        }

        let fork_name: ForkName = response
            .headers()
            .get(CONSENSUS_VERSION_HEADER)
            .ok_or_else(|| {
                anyhow!("Beacon API response missing {CONSENSUS_VERSION_HEADER} header")
            })?
            .to_str()
            .context("Eth-Consensus-Version header was not valid text")?
            .parse()
            .map_err(|error: String| anyhow!("unknown consensus version: {error}"))?;

        let bytes = response
            .bytes()
            .await
            .context("failed to read the beacon block body")?;
        histogram!(REQUEST_STAGE_DURATION, "stage" => "fetch")
            .record(fetch_start.elapsed().as_secs_f64());

        let decode_start = Instant::now();
        let block = SignedBeaconBlock::from_ssz_bytes_by_fork(&bytes, fork_name)
            .map_err(|e| anyhow!("failed to decode beacon block as {fork_name}: {e:?}"))?;
        histogram!(REQUEST_STAGE_DURATION, "stage" => "ssz_decode")
            .record(decode_start.elapsed().as_secs_f64());

        Ok(FetchedBlock {
            fork: fork_name,
            block,
        })
    }

    /// Fetches the node's spec configuration (`GET /eth/v1/config/spec`) as
    /// lighthouse's standard [`Config`], so the parsing quirks (quoted
    /// numbers, optional fork epochs, blob schedules) stay maintained
    /// upstream. The endpoint serves config+preset merged; [`Config`] decodes
    /// the config half and ignores the preset keys.
    pub async fn get_config(&self) -> Result<Config> {
        self.get_json("/eth/v1/config/spec").await
    }

    /// Fetches the chain's genesis time (`GET /eth/v1/beacon/genesis`).
    pub async fn get_genesis_time(&self) -> Result<u64> {
        let genesis: GenesisData = self.get_json("/eth/v1/beacon/genesis").await?;
        Ok(genesis.genesis_time)
    }

    /// Fetches a JSON endpoint and unwraps the Beacon API `data` envelope.
    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        #[derive(Deserialize)]
        struct Envelope<T> {
            data: T,
        }

        let url = self
            .endpoint
            .join(path)
            .with_context(|| format!("failed to construct the {path} URL"))?;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .context("failed to reach the Beacon API")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("Beacon API returned {status} for {path}: {body}");
        }
        let body: Envelope<T> = response
            .json()
            .await
            .with_context(|| format!("failed to decode the {path} response"))?;
        Ok(body.data)
    }

    /// Subscribes to the Beacon API `block` event stream
    /// (`GET /eth/v1/events?topics=block`).
    pub fn subscribe_block_events(&self) -> impl Stream<Item = Result<BlockEvent>> + Send + '_ {
        try_stream! {
            let mut url = self
                .endpoint
                .join("/eth/v1/events")
                .context("failed to construct beacon events URL")?;
            url.query_pairs_mut().append_pair("topics", "block");

            let mut events = EventSource::new(self.http.get(url))
                .map_err(|e| anyhow!("failed to open the beacon event stream: {e}"))?;

            while let Some(event) = events.next().await {
                match event {
                    Ok(SseEvent::Open) => {}
                    Ok(SseEvent::Message(message)) if message.event == "block" => {
                        let block_event: BlockEvent = serde_json::from_str(&message.data)
                            .context("failed to decode a beacon block event")?;
                        yield block_event;
                    }
                    Ok(SseEvent::Message(_)) => {}
                    Err(error) => {
                        events.close();
                        Err(anyhow!("beacon event stream error: {error}"))?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_header_map_parses_and_skips_blanks() {
        let map = build_header_map(&["X-API-Key: secret".to_string(), "   ".to_string()])
            .expect("valid headers");
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("x-api-key").expect("header present"), "secret");
    }

    #[test]
    fn build_header_map_rejects_missing_colon_without_echoing_the_value() {
        let error = build_header_map(&["X-API-Key hunter2".to_string()])
            .expect_err("missing colon must be rejected");
        // A colonless entry cannot be split into name and value, so the whole
        // string — possibly a secret — must stay out of the error.
        assert!(!format!("{error:#}").contains("hunter2"));
    }
}
