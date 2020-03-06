import sys
import requests

REPO='JasonAlt/GridFTP-DSI-for-HPSS'

def get_release_info(repo, tag):
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    r = requests.get(url)
    if r.status_code != 200:
        raise SystemExit(f'Failed to get GitHub release information:\n{r.text}')
    return r.json()

def get_download_url(info, asset):
    for a in info['assets']:
       if a['name'] == asset:
            return a['browser_download_url']
    return None

def download_asset(repo, tag, asset):
    info = get_release_info(repo, tag)
    url = get_download_url(info, asset)
    r = requests.get(url, allow_redirects=True)
    open(asset, 'wb').write(r.content)

def usage(argv):
    usage = f'Usage: {sys.argv[0]} <tag> <asset>'

    if len(argv) != 3:
        raise SystemExit(usage)
    return { 'tag': argv[1], 'asset': argv[2] }

if __name__ == "__main__":
    config = usage(sys.argv)
    download_asset(REPO, **config)
