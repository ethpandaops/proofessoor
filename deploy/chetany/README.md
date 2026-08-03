# chetany learning droplets

Temporary infra for learning/experimentation: 3x DigitalOcean droplets
(`s-4vcpu-8gb`, Debian 13, one each in ams3/nyc1/sgp1) with DNS on
`ethpandaops.io` and a minimal ansible bootstrap that creates the `devops`
user.

## Terraform

State lives in a private ethPandaOps-managed bucket. The backend settings are
not committed to this public repo — copy `terraform/backend.conf.example` to
`terraform/backend.conf` and fill in the real values (ask the ethPandaOps
team), then:

```sh
cd terraform
terraform init -backend-config=backend.conf
terraform apply
```

Requires DigitalOcean Spaces credentials (`AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`), a `DIGITALOCEAN_TOKEN` and
`TF_VAR_cloudflare_api_token` in the environment.

The apply also generates `ansible/inventories/inventory.ini` (gitignored, as
it contains the droplet IPs).

## Ansible

Bootstraps the machines and creates the `devops` user with SSH keys pulled
from the GitHub handles listed in
`ansible/inventories/group_vars/all/defaults.yaml`. Nothing else is managed.

```sh
cd ansible
./install_dependencies.sh
ansible-playbook playbook.yaml
```

The first run connects as `root`; after that, connections use the `devops`
user.
