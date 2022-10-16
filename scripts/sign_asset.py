import gnupg
import sys

def sign_asset(asset):
    gpg = gnupg.GPG(verbose=9, homedir='/home/jason/.gnupg')
    stream = open(asset, "rb")
    with open(asset, "rb") as stream:
        signed_data = gpg.sign(stream, clearsign=False, detach=True)#, default_key='C36C826C18ED73C338DCFA531EA106A24003C353')
    with open(asset + '.asc', "wb") as stream:
        stream.write(signed_data.data)

def usage(argv):
    usage = f'Usage: {sys.argv[0]} <asset>'

    if len(argv) != 2:
        raise SystemExit(usage)
    return { 'asset': argv[1] }

if __name__ == "__main__":
    config = usage(sys.argv)
    sign_asset(**config)
