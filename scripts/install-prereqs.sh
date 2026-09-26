#!/usr/bin/env bash
#
# Instalacao OPCIONAL dos pre-requisitos do projeto, para WSL2 (Ubuntu) ou Linux.
set -u

KIND_VERSION="v0.24.0"

FALTANDO=""
FALHARAM=""

have() { command -v "$1" >/dev/null 2>&1; }

arch_suffix() {
    case "$(uname -m)" in
        x86_64)  echo "amd64" ;;
        aarch64) echo "arm64" ;;
        *)       echo "" ;;
    esac
}

echo "=== Verificando pre-requisitos ==="
for cmd in docker kind kubectl helm make unzip; do
    if have "$cmd"; then
        printf '  presente : %s\n' "$cmd"
    else
        printf '  FALTA    : %s\n' "$cmd"
        FALTANDO="$FALTANDO $cmd"
    fi
done
echo

if ! have docker; then
    echo "AVISO: 'docker' nao encontrado, e este script NAO instala Docker."
    echo "  No Windows, instale o Docker Desktop e habilite a integracao com o WSL:"
    echo "    https://learn.microsoft.com/windows/wsl/install"
    echo "    https://docs.docker.com/desktop/features/wsl/"
    echo "  No Linux, instale o Docker Engine pela documentacao oficial:"
    echo "    https://docs.docker.com/engine/install/"
    echo
fi

INSTALAVEIS=""
for cmd in $FALTANDO; do
    [ "$cmd" = "docker" ] && continue
    INSTALAVEIS="$INSTALAVEIS $cmd"
done

if [ -z "${INSTALAVEIS// /}" ]; then
    echo "Nada a instalar por este script."
    echo
else
    echo "Este script vai instalar:$INSTALAVEIS"
    echo "Alguns passos usam 'sudo' (instalacao em /usr/local/bin e via apt)."
    printf "Continuar? [s/N] "
    read -r RESP
    case "${RESP:-n}" in
        s|S|y|Y) ;;
        *) echo "Cancelado pelo usuario."; exit 0 ;;
    esac
    echo

    ARCH="$(arch_suffix)"
    if [ -z "$ARCH" ]; then
        echo "AVISO: arquitetura $(uname -m) nao reconhecida; kind/kubectl podem falhar."
    fi

    TMPDIR_LOCAL="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR_LOCAL"' EXIT

    for cmd in $INSTALAVEIS; do
        echo "--- instalando $cmd ---"
        case "$cmd" in
            make|unzip)
                sudo apt-get update -qq && sudo apt-get install -y "$cmd" \
                    || FALHARAM="$FALHARAM $cmd"
                ;;
            kind)
                curl -fsSLo "$TMPDIR_LOCAL/kind" \
                    "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}" \
                    && sudo install -m 0755 "$TMPDIR_LOCAL/kind" /usr/local/bin/kind \
                    || FALHARAM="$FALHARAM kind"
                ;;
            kubectl)
                KVER="$(curl -fsSL https://dl.k8s.io/release/stable.txt)" \
                    && curl -fsSLo "$TMPDIR_LOCAL/kubectl" \
                       "https://dl.k8s.io/release/${KVER}/bin/linux/${ARCH}/kubectl" \
                    && sudo install -m 0755 "$TMPDIR_LOCAL/kubectl" /usr/local/bin/kubectl \
                    || FALHARAM="$FALHARAM kubectl"
                ;;
            helm)
                # Script oficial do projeto Helm; ele mesmo pede sudo quando precisa.
                curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \
                    | bash || FALHARAM="$FALHARAM helm"
                ;;
        esac
        echo
    done
fi

echo "=== Versoes instaladas ==="
have docker  && printf '  docker  : %s\n' "$(docker --version 2>/dev/null)"   || echo "  docker  : ausente"
have kind    && printf '  kind    : %s\n' "$(kind --version 2>/dev/null)"     || echo "  kind    : ausente"
have kubectl && printf '  kubectl : %s\n' "$(kubectl version --client 2>/dev/null | head -1)" || echo "  kubectl : ausente"
have helm    && printf '  helm    : %s\n' "$(helm version --short 2>/dev/null)" || echo "  helm    : ausente"
have make    && printf '  make    : %s\n' "$(make --version 2>/dev/null | head -1)" || echo "  make    : ausente"
have unzip   && printf '  unzip   : presente\n' || echo "  unzip   : ausente"
echo

if [ -n "${FALHARAM// /}" ]; then
    echo "ATENCAO: falharam ->$FALHARAM"
    echo "Instale-os manualmente antes de rodar 'make setup'."
    exit 1
fi

echo "Pre-requisitos prontos. Proximo passo: ver a secao 'Como rodar' do README."
