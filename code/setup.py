import os


def prep_ubuntu():
    # UBUNTU PREPPING
    apt_packages = [
        'python3.7',
        'python3-pip',
        'build-essential unixodbc-dev',
        'virtualenv'
    ]

    os.system('sudo apt update && sudo apt upgrade -y')
    for package in apt_packages:
        os.system('sudo apt install -y %s' % package)
    return True


def prep_env():
    commands = [
        'pip3 install virtualenv',
        'virtualenv -p python3 ../venv',
        'source ../venv/bin/activate',
        'pip install -r code/requirements.txt'
    ]
    for command in commands:
        os.system(command)
    return True


def main():
    prep_ubuntu()
    prep_env()
    return "Done"


main()
