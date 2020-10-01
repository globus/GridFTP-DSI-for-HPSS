REPO='JasonAlt/GridFTP-DSI-for-HPSS'

function get_release_info {
    repo=$1
    tag=$2

    url="https://api.github.com/repos/${repo}/releases/tags/${tag}"
    curl --location --silent $url
}

function get_release_id {
    info=$1
    echo $1 | jq .id
}

# There is a small window after uploading an asset where this function will
# think the asset has not been uploaded. Must be a GitHub eventual consistency
# type of thing.
function is_asset_uploaded {
    release_info=$1
    asset_path=$2
    asset_name=`basename $asset_path`

    match=`echo $release_info | jq ".assets[] | select(.name==\"${asset_name}\")"`
    [[ -z "$match" ]] || return 0
    return 1
}

function upload_asset {
    repo=$1
    tag=$2
    token=$3
    asset=$4

    release_info=`get_release_info $repo $tag`
    release_id=`get_release_id "$release_info"`

    if is_asset_uploaded "$release_info" "$asset"
    then
        echo "already uploaded, skipping"
        return 0
    fi

    url="https://uploads.github.com/repos/${repo}/releases/${release_id}/assets?name=$(basename $asset)"
    curl --header "Content-Type: multipart/form-data" \
         --header "Authorization: token ${token}"     \
         --location --post301                         \
         --data-binary "@${asset}"                    \
         $url
    return $?
}

TAG=$1
ASSET=$2

if [[ -z "${GITHUB_TOKEN}" || -z "${TAG}" || -z "${ASSET}" ]]
then
    echo "Usage: GITHUB_TOKEN=<token> upload_asset.sh <tag> <asset>"
    [[ $_ != $0 ]] && return 1 || exit 1
fi

upload_asset $REPO $TAG $GITHUB_TOKEN $ASSET
