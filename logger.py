from settings import log_level

def log(level, *arg):
    if log_level >= level:
        print(*arg)

def log_color(level, color, *arg):
    colors = {
            "black":"\033[30m",
            "red":"\033[31m",
            "green":"\033[32m",
            "yellow":"\033[33m",
            "blue":"\033[34m",
            "magenta":"\033[35m",
            "cyan":"\033[36m",
            "white":"\033[37m"
            }
    print(colors[color], end="")
    log(level, *arg, "\033[0m")
