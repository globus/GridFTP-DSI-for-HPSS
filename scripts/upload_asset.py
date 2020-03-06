import os
import sys
import requests
import mimetypes

REPO='JasonAlt/GridFTP-DSI-for-HPSS'

def get_release_info(repo, tag):
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    r = requests.get(url)
    if r.status_code != 200:
        raise SystemExit(f'Failed to get GitHub release information:\n{r.text}')
    return r.json()

def get_release_id(info):
    return info['id']

def is_asset_uploaded(info, asset):
    for a in info['assets']:
       if a['name'] == asset:
            return True
    return False

def upload_asset(repo, tag, token, asset):
    info = get_release_info(repo, tag)
    id = get_release_id(info)

    if is_asset_uploaded(info, asset):
        print (f'already uploaded, skipping')
        return

    url = f'https://uploads.github.com/repos/{repo}/releases/{id}/assets?name={asset}'
    headers = {'Authorization': f'token {token}', 'Content-Type': mimetypes.guess_type(asset)[0]}
    data = open(asset, 'rb').read()
    r = requests.post(url, data=data, headers=headers)
    if r.status_code != 201:
        raise SystemExit(f'Failed to upload {asset}:\n{r.text}')

def usage(argv):
    usage = f'Usage: GITHUB_TOKEN=<token> {sys.argv[0]} <tag> <asset>'

    token = os.getenv("GITHUB_TOKEN")
    if token is None:
        sys.exit(usage)

    if len(argv) != 3:
        raise SystemExit(usage)
    return { 'tag': argv[1], 'asset': argv[2], 'token': token }

if __name__ == "__main__":
    config = usage(sys.argv)
    upload_asset(REPO, **config)

