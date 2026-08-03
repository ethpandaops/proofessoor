# chetany learning droplets

Minimal ansible bootstrap for the chetany learning droplets: creates the
`devops` user with SSH keys pulled from the GitHub handles listed in
`ansible/inventories/group_vars/all/defaults.yaml`, plus base packages and
oh-my-zsh. Nothing else is managed.

The droplets themselves (3x DigitalOcean `s-4vcpu-8gb`, Debian 13, one each
in ams3/nyc1/sgp1) are managed by terraform in the private ethPandaOps repo,
which also generates `ansible/inventories/inventory.ini` (committed here).

## Usage

```sh
cd ansible
./install_dependencies.sh
ansible-playbook playbook.yaml
```

The first run connects as `root`; after that, connections use the `devops`
user.
