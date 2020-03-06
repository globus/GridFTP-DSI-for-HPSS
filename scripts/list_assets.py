import os
import sys
import json
import requests

REPO='JasonAlt/GridFTP-DSI-for-HPSS'

def get_release_info(repo, tag):
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    r = requests.get(url)
    if r.status_code != 200:
        raise SystemExit(f'Failed to get GitHub release information:\n{r.text}')
    return r.json()

def get_asset_list(info):
    l = []
    for a in info['assets']:
        l.append(a['name'])
    return l

def std_display(assets):
    for a in assets:
        print(a)

def json_display(assets):
        print (json.dumps(assets))

def display_assets(assets):
    if sys.stdout.isatty():
        std_display (assets)
    else:
        json_display(assets)

def list_assets(repo, tag):
    info = get_release_info(repo, tag)
    assets = get_asset_list(info)
    display_assets(assets)

def usage(argv):
    usage = f'Usage: {sys.argv[0]} <tag>'

    if len(argv) != 2:
        raise SystemExit(usage)
    return { 'tag': argv[1] }

if __name__ == "__main__":
    config = usage(sys.argv)
    list_assets(REPO, **config)
