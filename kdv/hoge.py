import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="MyLearn CLI")
    parser.parse_args()
    print("Hello, world!")

    hoge = "hoge"
    hoge = 77


if __name__ == "__main__":
    main()
