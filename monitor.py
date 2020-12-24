import curses


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

    def teardown(self):
        self.screen.keypad(0)
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def listen(self):
        self.screen.getch()

    def init(self, batch_size):
        self.screen.addstr(0, 0, "🦀 decapode crawling...", curses.A_BOLD)
        self.screen.addstr(1, 0, f"batch size: {batch_size}")
        self.screen.refresh()

    def set_status(self, status):
        self.screen.move(3, 0)
        self.screen.clrtoeol()
        self.screen.addstr(3, 0, status, curses.A_REVERSE)
        self.screen.refresh()

    def refresh(self, results):
        # start line after init and status
        START = 5

        for i, status in enumerate(['ok', 'timeout', 'error']):
            nb = len([r for r in results if r['status'] == status])
            self.screen.addstr(i + START, 0, f'{status}:')
            self.screen.addstr(i + START, 10, f'{nb}')

        self.screen.refresh()
