import os
import sys
import requests
import mimetypes

#{
#  'url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/releases/23738209',
#  'assets_url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/releases/23738209/assets',
#  'upload_url': 'https://uploads.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/releases/23738209/assets{?name,label}',
#  'html_url': 'https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/releases/tag/Version_2_10_1',
#  'id': 23738209,
#  'node_id': 'MDc6UmVsZWFzZTIzNzM4MjA5',
#  'tag_name': 'Version_2_10_1',
#  'target_commitish': 'master',
#  'name': 'Version 2.10.1',
#  'draft': False,
#  'author': {
#     'login': 'JasonAlt',
#     'id': 1641298,
#      'node_id': 'MDQ6VXNlcjE2NDEyOTg=',
#      'avatar_url': 'https://avatars2.githubusercontent.com/u/1641298?v=4',
#      'gravatar_id': '',
#      'url': 'https://api.github.com/users/JasonAlt',
#      'html_url': 'https://github.com/JasonAlt',
#      'followers_url': 'https://api.github.com/users/JasonAlt/followers',
#      'following_url': 'https://api.github.com/users/JasonAlt/following{/other_user}',
#      'gists_url': 'https://api.github.com/users/JasonAlt/gists{/gist_id}',
#      'starred_url': 'https://api.github.com/users/JasonAlt/starred{/owner}{/repo}',
#      'subscriptions_url': 'https://api.github.com/users/JasonAlt/subscriptions',
#      'organizations_url': 'https://api.github.com/users/JasonAlt/orgs',
#      'repos_url': 'https://api.github.com/users/JasonAlt/repos',
#      'events_url': 'https://api.github.com/users/JasonAlt/events{/privacy}',
#      'received_events_url': 'https://api.github.com/users/JasonAlt/received_events',
#      'type': 'User',
#      'site_admin': False
#    },
#   'prerelease': False,
#   'created_at': '2020-03-05T19:47:31Z',
#   'published_at': '2020-02-17T15:47:31Z',
#   'assets':
#      [
#         {
#            'url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/releases/assets/18505484',
#            'id': 18505484,
#            'node_id': 'MDEyOlJlbGVhc2VBc3NldDE4NTA1NDg0',
#            'name': 'globus-gridftp-server-hpss-7.4-2.10-1.el7.x86_64.rpm',
#            'label': '',
#            'uploader': {
#               'login': 'JasonAlt',
#               'id': 1641298,
#               'node_id': 'MDQ6VXNlcjE2NDEyOTg=',
#               'avatar_url': 'https://avatars2.githubusercontent.com/u/1641298?v=4',
#               'gravatar_id': '',
#               'url': 'https://api.github.com/users/JasonAlt',
#               'html_url': 'https://github.com/JasonAlt',
#               'followers_url': 'https://api.github.com/users/JasonAlt/followers',
#               'following_url': 'https://api.github.com/users/JasonAlt/following{/other_user}',
#               'gists_url': 'https://api.github.com/users/JasonAlt/gists{/gist_id}',
#               'starred_url': 'https://api.github.com/users/JasonAlt/starred{/owner}{/repo}',
#               'subscriptions_url': 'https://api.github.com/users/JasonAlt/subscriptions',
#               'organizations_url': 'https://api.github.com/users/JasonAlt/orgs',
#               'repos_url': 'https://api.github.com/users/JasonAlt/repos',
#               'events_url': 'https://api.github.com/users/JasonAlt/events{/privacy}',
#               'received_events_url': 'https://api.github.com/users/JasonAlt/received_events',
#               'type': 'User',
#               'site_admin': False
#            },
#            'content_type': 'application/x-rpm',
#            'state': 'uploaded',
#            'size': 49324,
#            'download_count': 0,
#            'created_at': '2020-03-05T20:50:10Z',
#            'updated_at': '2020-03-05T20:50:11Z',
#            'browser_download_url': 'https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/releases/download/Version_2_10_1/globus-gridftp-server-hpss-7.4-2.10-1.el7.x86_64.rpm'
#         },
#         {
#            'url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/releases/assets/18505486',
#            'id': 18505486,
#            'node_id': 'MDEyOlJlbGVhc2VBc3NldDE4NTA1NDg2',
#            'name': 'globus-gridftp-server-hpss-7.4-debuginfo-2.10-1.el7.x86_64.rpm',
#            'label': '',
#            'uploader': {
#               'login': 'JasonAlt',
#               'id': 1641298,
#               'node_id': 'MDQ6VXNlcjE2NDEyOTg=',
#               'avatar_url': 'https://avatars2.githubusercontent.com/u/1641298?v=4',
#               'gravatar_id': '',
#               'url': 'https://api.github.com/users/JasonAlt',
#               'html_url': 'https://github.com/JasonAlt',
#               'followers_url': 'https://api.github.com/users/JasonAlt/followers',
#               'following_url': 'https://api.github.com/users/JasonAlt/following{/other_user}',
#               'gists_url': 'https://api.github.com/users/JasonAlt/gists{/gist_id}',
#               'starred_url': 'https://api.github.com/users/JasonAlt/starred{/owner}{/repo}',
#               'subscriptions_url': 'https://api.github.com/users/JasonAlt/subscriptions',
#               'organizations_url': 'https://api.github.com/users/JasonAlt/orgs',
#               'repos_url': 'https://api.github.com/users/JasonAlt/repos',
#               'events_url': 'https://api.github.com/users/JasonAlt/events{/privacy}',
#               'received_events_url': 'https://api.github.com/users/JasonAlt/received_events',
#               'type': 'User',
#               'site_admin': False
#             },
#             content_type': 'application/x-rpm',
#             state': 'uploaded',
#             size': 92168,
#             download_count': 0,
#             created_at': '2020-03-05T20:50:11Z',
#             updated_at': '2020-03-05T20:50:11Z',
#             browser_download_url': 'https://github.com/JasonAlt/GridFTP-DSI-for-HPSS/releases/download/Version_2_10_1/globus-gridftp-server-hpss-7.4-debuginfo-2.10-1.el7.x86_64.rpm'
#         }
#      ],
#      'tarball_url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/tarball/Version_2_10_1',
#      'zipball_url': 'https://api.github.com/repos/JasonAlt/GridFTP-DSI-for-HPSS/zipball/Version_2_10_1',
#      'body': '- Fixes #72: WebApp should be able to follow symlinks to directories'
#}


def get_release_info(repo, tag):
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    r = requests.get(url)
    if r.status_code != 200:
        raise SystemExit(f'Failed to get GitHub release information:\n{r.text}')
    return r.json()

def get_release_id(info):
    return info['id']

def is_pkg_uploaded(info, pkg):
    for a in info['assets']:
       if a['name'] == pkg:
            return True
    return False

def main(version, github_token, packages):
    repo = 'JasonAlt/GridFTP-DSI-for-HPSS'
    info = get_release_info(repo, version)
    id = get_release_id(info)

    for pkg in packages:
        print (f'{pkg} ... ', end='')
        if is_pkg_uploaded(info, pkg):
            print (f'already uploaded, skipping')
            continue

        url = f'https://uploads.github.com/repos/{repo}/releases/{id}/assets?name={pkg}'
        headers = {'Authorization': f'token {github_token}', 'Content-Type': mimetypes.guess_type(pkg)[0]}
        data = open(pkg, 'rb').read()
        r = requests.post(url, data=data, headers=headers)
        if r.status_code != 201:
            raise SystemExit(f'Failed to upload {pkg}:\n{r.text}')
        print ('done')


if __name__ == "__main__":
    usage = f'Usage: RELEASE=<tag> GITHUB_TOKEN=<token> {sys.argv[0]} <package1> [package2...]'

    github_token = os.getenv("GITHUB_TOKEN")
    if github_token is None:
        sys.exit(usage)

    release = os.getenv("RELEASE")
    if release is None:
        sys.exit(usage)

    main(release, github_token, sys.argv[1:])
