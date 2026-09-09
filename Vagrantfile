# frozen_string_literal: true

ENV["VAGRANT_DEFAULT_PROVIDER"] ||= "parallels"

Vagrant.configure("2") do |config|
  # The official AlmaLinux 9 box provides an ARM64 Parallels image for
  # Apple Silicon. The project checkout is available at /vagrant.
  config.vm.box = "almalinux/9"
  config.vm.hostname = "piddi-alma"

  config.vm.provider "parallels" do |prl|
    prl.name = "piddiplatsch-almalinux9"
    prl.memory = 4096
    prl.cpus = 4
  end

  config.vm.provision "shell", privileged: true, inline: <<~SHELL
    set -eu

    dnf install -y dnf-plugins-core
    dnf config-manager --set-enabled crb
    dnf install -y epel-release
    dnf install -y \
      ansible-core \
      byobu \
      git \
      make \
      python3.11 \
      python3.11-pip \
      vim-enhanced

    echo "Development VM ready. Repository: /vagrant"
  SHELL
end
