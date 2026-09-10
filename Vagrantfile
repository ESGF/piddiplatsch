# frozen_string_literal: true

require "rbconfig"

Vagrant.configure("2") do |config|
  # The official AlmaLinux 9 box provides Parallels for ARM64 and
  # VirtualBox/libvirt for x86_64. The checkout is available at /vagrant.
  config.vm.box = "almalinux/9"
  config.vm.hostname = "piddi-alma"

  host_os = RbConfig::CONFIG["host_os"]
  host_cpu = RbConfig::CONFIG["host_cpu"]
  apple_silicon = host_os.include?("darwin") && %w[arm64 aarch64].include?(host_cpu)

  if apple_silicon
    config.vm.provider "parallels" do |prl|
      prl.name = "piddiplatsch-almalinux9"
      prl.memory = 4096
      prl.cpus = 4
    end
  else
    if host_os.include?("linux")
      config.vm.provider "libvirt" do |libvirt|
        libvirt.memory = 4096
        libvirt.cpus = 4
      end
    end

    config.vm.provider "virtualbox" do |vb|
      vb.name = "piddiplatsch-almalinux9"
      vb.memory = 4096
      vb.cpus = 4
    end
  end

  config.vm.provision "shell", privileged: true, inline: <<~SHELL
    set -eu

    dnf install -y dnf-plugins-core
    dnf config-manager --set-enabled crb
    dnf install -y epel-release
    dnf install -y \
      ansible-core \
      byobu \
      ca-certificates \
      curl \
      git \
      make \
      python3.11 \
      python3.11-pip \
      vim-enhanced

    miniforge_root=/opt/conda
    miniforge_version=26.7.2-0

    if [ ! -x "${miniforge_root}/bin/conda" ]; then
      if [ -e "${miniforge_root}" ]; then
        echo "Incomplete Miniforge installation found at ${miniforge_root}" >&2
        exit 1
      fi

      case "$(uname -m)" in
        aarch64)
          miniforge_arch=aarch64
          miniforge_sha256=89b786c8d2c8b0fda7553914c1314ae4ddaa094503802f279377b19ac4463cb2
          ;;
        x86_64)
          miniforge_arch=x86_64
          miniforge_sha256=281b0ac7d550802efc81af633225a5e6116d29ae72f3ab4eae7168c3931a4c05
          ;;
        *)
          echo "Unsupported Miniforge architecture: $(uname -m)" >&2
          exit 1
          ;;
      esac

      # Miniforge verifies that the invoked installer filename ends in .sh.
      miniforge_installer="$(mktemp --suffix=.sh)"
      trap 'rm -f "${miniforge_installer}"' EXIT
      miniforge_url="https://github.com/conda-forge/miniforge/releases/download/${miniforge_version}/Miniforge3-${miniforge_version}-Linux-${miniforge_arch}.sh"
      curl --fail --location --silent --show-error \
        --output "${miniforge_installer}" "${miniforge_url}"
      echo "${miniforge_sha256}  ${miniforge_installer}" | sha256sum --check --status
      bash "${miniforge_installer}" -b -p "${miniforge_root}"
    fi

    printf '%s\n' 'export PATH="/opt/conda/bin:$PATH"' > /etc/profile.d/miniforge.sh
    chmod 0644 /etc/profile.d/miniforge.sh

    echo "Development VM ready. Miniforge: /opt/conda; repository: /vagrant"
  SHELL
end
