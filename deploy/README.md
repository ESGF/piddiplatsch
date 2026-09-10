# Deployment

Install Conda on the host first; Miniforge is recommended. This remains a
manual host-administration step in production.

Check out Piddi manually, normally below `/opt/piddiplatsch`, then create its
configuration and run the complete deployment:

```console
cd /opt/piddiplatsch
python -m pip install ansible-core
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make deploy
```

`make deploy` creates or updates `.conda`, installs Piddi, and runs Ansible.
Use `make play` for later Ansible-only configuration changes.

Run this as root or with passwordless sudo. If Ansible needs a sudo password,
use `make play ANSIBLE_ARGS=--ask-become-pass`.

Ansible creates the service user and directories, renders
`/etc/piddi/piddi.toml`, and validates it, but does not start Piddi. Test it:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml --silent consume
```

Then set `piddi_enable_service: true` in `custom.yml` and run `make play` again.
Check that Piddi is working:

```console
systemctl status piddi
piddi top
tail -f /var/log/piddi/piddi.log
```

## Vagrant test VM

Use one of these host/provider combinations:

- Apple Silicon macOS: Parallels
- Intel macOS: VirtualBox
- x86_64 Linux: libvirt, with VirtualBox as a fallback

Install the matching Vagrant provider when needed, then start the VM:

```console
# Apple Silicon
vagrant plugin install vagrant-parallels
vagrant up --provider=parallels

# Intel macOS
vagrant up --provider=virtualbox

# Linux
vagrant plugin install vagrant-libvirt
vagrant up --provider=libvirt

vagrant ssh
```

The VM provisioning installs Miniforge under `/opt/conda`. For an already
running VM after a Vagrantfile change, run `vagrant provision` once.

Inside the VM, perform the complete deployment as root:

```console
sudo -i
git clone https://github.com/ESGF/piddiplatsch.git /opt/piddiplatsch
cd /opt/piddiplatsch
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make deploy
```

`conda init` is not needed because the Make target addresses the environment
by its prefix. For optional interactive activation in the current root shell, run
`source /opt/conda/etc/profile.d/conda.sh` followed by
`conda activate /opt/piddiplatsch/.conda`.

Remove the VM later with `vagrant destroy` on the Mac.
