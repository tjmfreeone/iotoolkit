# @Author : taojinmin
# @Time : 2023/2/7 18:42


class FuncSet:
    min_secs = 60
    hour_secs = 3600
    day_secs = 3600 * 24

    @classmethod
    def x2humansTime(cls, secs):
        h_time = ""
        if secs < cls.min_secs:
            secs = "%.3f" % secs
            h_time = "{}s".format(secs)
        elif cls.min_secs <= secs < cls.hour_secs:
            mins, secs = secs // cls.min_secs, secs % cls.min_secs
            secs = "%.3f" % secs
            h_time = "{}m{}s".format(int(mins), secs)
        elif cls.hour_secs <= secs < cls.day_secs:
            hours, secs = secs // cls.hour_secs, secs % cls.hour_secs
            mins, secs = secs // cls.min_secs, secs % cls.min_secs
            secs = "%.3f" % secs
            h_time = "{}h{}m{}s".format(int(hours), int(mins), secs)
        else:
            days, secs = secs // cls.day_secs, secs % cls.day_secs
            hours, secs = secs // cls.hour_secs, secs % cls.hour_secs
            mins, secs = secs // cls.min_secs, secs % cls.min_secs
            secs = "%.3f" % secs
            h_time = "{}d{}h{}m{}s".format(int(days), int(hours), int(mins), secs)
        return h_time
