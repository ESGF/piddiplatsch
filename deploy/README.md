# Deployment

Install Conda on the host first; Miniforge is recommended. This remains a
manual host-administration step in production.

Check out Piddi manually, normally below `/opt/piddiplatsch`, then create its
project-local Conda environment and install Piddi into it:

```console
cd /opt/piddiplatsch
conda env create --prefix .conda --file environment.yml
conda run --prefix .conda python -m pip install -e .
```

Configure its executable and service:

```console
python -m pip install ansible-core
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make play
```

Ansible creates the service user and directories, renders
`/etc/piddi/piddi.toml`, and validates it, but does not start Piddi. Test it:

```console
sudo runuser -u piddi -- /opt/piddiplatsch/.conda/bin/piddi \
  --config /etc/piddi/piddi.toml --silent consume
```

Then set `piddi_enable_service: true` in `custom.yml` and run `make play` again.

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
running VM after a Vagrantfile change, run `vagrant provision` once. Then check
out Piddi under `/opt/piddiplatsch` and perform the Conda setup above manually.

Run the local Ansible deployment from that checkout:

```console
cd /opt/piddiplatsch
cp deploy/ansible/custom.yml.example deploy/ansible/custom.yml
vim deploy/ansible/custom.yml
make play ANSIBLE_ARGS=
```

Remove the VM later with `vagrant destroy` on the Mac.
