import datetime


class ClockAloe:
    def __init__(self, hours, minutes, seconds):
        self._hours = hours
        self._minutes = minutes
        self._seconds = seconds
        self._last_hours_15m = None
        self._last_minutes_15m = None
        self._last_seconds_15m = None
        self._last_hours_heure = None
        self._last_minutes_heure = None
        self._last_seconds_heure = None

    def set_clock(self, hours, minutes, seconds):
        self._hours = hours
        self._minutes = minutes
        self._seconds = seconds

    def set_clock_15m(self, hours, minutes, seconds):
        self._last_hours_15m = hours
        self._last_minutes_15m = minutes
        self._last_seconds_15m = seconds

    def set_clock_heure(self, hours, minutes, seconds):
        self._last_hours_heure = hours
        self._last_minutes_heure = minutes
        self._last_seconds_heure = seconds

    def get_clock(self):
        return '{0:%H:%M:%S}'.format(datetime.time(self._hours, self._minutes, self._seconds ))

    def get_clock_15m(self):
        return '{0:%H:%M:%S}'.format(datetime.time(self._last_hours_15m, self._last_minutes_15m, self._last_seconds_15m))

    def get_clock_heure(self):
        return '{0:%H:%M:%S}'.format(datetime.time(self._last_hours_heure, self._last_minutes_heure, self._last_seconds_heure))

    def __str__(self):
        return '{0:%H:%M:%S}'.format(datetime.time(self._hours, self._minutes, self._seconds))

    def add_second(self):
        self._seconds += 1

# if __name__ == '__main__':
#     c = ClockAloe(9, 39, 25)
#     print(c)
#     c.add_second()
#     print(c)


