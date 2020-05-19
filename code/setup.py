import os


def prep_ubuntu():
    # UBUNTU PREPPING
    apt_packages = [
        'python3.7',
        '-y python3-pip',
        'build-essential unixodbc-dev',
    ]

    os.system('sudo apt update && sudo apt upgrade -y')
    for package in apt_packages:
        os.system('sudo apt install %s' % package)
    return True


def prep_env():
    commands = [
        'pip install virtualenv',
        'virtualenv -p python3 ../../venv',
        '. ../../etl_venv/bin/activate',
        'pip install -r requirements.txt'
    ]
    for command in commands:
        os.system(command)
    return True


def main():
    prep_ubuntu()
    prep_env()
    return "Done"


main()
