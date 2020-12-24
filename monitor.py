import curses
from collections import defaultdict


class Monitor():
    """curses-based monitor interface"""

    def __init__(self):
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.screen.keypad(1)
        # hide cursor
        curses.curs_set(0)
        try:
            curses.start_color()
        except:  # noqa
            pass
        self.backoffs = defaultdict(int)

    def teardown(self):
        self.screen.keypad(0)
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def listen(self):
        self.screen.getch()

    def clear_line(self, line):
        self.screen.move(line, 0)
        self.screen.clrtoeol()

    def init(self, batch_size):
        self.screen.addstr(0, 0, "🦀 decapode crawling...", curses.A_BOLD)
        self.screen.addstr(1, 0, f"batch size: {batch_size}")
        self.screen.refresh()

    def set_status(self, status):
        START = 3
        self.clear_line(START)
        self.screen.addstr(START, 0, status, curses.A_REVERSE)
        self.screen.refresh()

    def refresh(self, results):
        # start line after init and status
        START = 5

        # TODO: use config vars from crawl.py
        for i, status in enumerate(['ok', 'timeout', 'error']):
            self.screen.addstr(i + START, 0, f'{status}:')
            self.screen.addstr(i + START, 10, f"{results[status]}")

        self.screen.refresh()

    def draw_backoffs(self):
        START = 8
        self.clear_line(START)
        # TODO: check max width
        backoffs = [f'{d} ({c})' for d, c in self.backoffs.items()]
        msg = f"backoffs: {', '.join(backoffs)}"
        self.screen.addstr(START, 0, msg)
        self.screen.refresh()

    def add_backoff(self, domain):
        if domain not in self.backoffs:
            self.backoffs[domain] += 1
            self.draw_backoffs()

    def remove_backoff(self, domain):
        if domain in self.backoffs:
            self.backoffs.pop(domain)
            self.draw_backoffs()
