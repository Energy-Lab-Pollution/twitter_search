from twitter_search import run
from argparse import ArgumentParser


def main():

    try:
        parser = ArgumentParser(description="Get users from Twitter based on location and algorithm.")
        parser.add_argument("--location", type=str, help="Specify the location (city) for Twitter user search.")
        # parser.add_argument("--algorithm", type=int, choices=[1, 2], help="Specify the algorithm (1 or 2).")
        args = parser.parse_args()

        if not args.location:
            print("Please provide both --location and --algorithm arguments.")
            return
        else:
            location = args.location
            run.lets_getit(location)
    except:
        print("what magaaaaa")

if __name__ == '__main__':
    main()