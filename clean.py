'''
Install prereq:

$ pip install click
'''

import click
import os
import glob

@click.command()
@click.option("--all/--no-all","-a/-na", default=False, help="Clean all the replay files and log.")
@click.option("--log/--no-log","-l/-nl", default=False, help="Clean the log.")
@click.option("--replay/--no-replay","-r/-nr", default=False, help="Clean all replays.")
@click.option("--zipped/--no-zipped","-z/-nz", default=False, help="Clean the zipped replays.")
@click.option("--unzipped/--no-unzipped","-u/-nu", default=False, help="Clean the unzipped replays.")
@click.option("--details/--no-details","-d/-nd", default=False, help="Clean the match details.")
def launch(all, log, replay, zipped, unzipped, details):
    '''Clean the replay data files or logs without removing the important direcotry.
    '''
    if all:
        log = replay = zipped = unzipped = details = True

    if replay:
        zipped = unzipped = details= True

    if log:
        clean("./logs/log-*.txt")

    if zipped:
        clean("./test-data/replays/zipped/**/*.dem.bz2")

    if unzipped:
        clean("./test-data/replays/unzipped/**/*.dem")

    if details:
        clean("./test-data/match-details/*.json")


def clean(pattern):
    for i in glob.glob(pattern):
        os.remove(i)

if __name__ == "__main__":
    launch()
