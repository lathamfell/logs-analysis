#!/usr/bin/python3

import psycopg2


class LogsAnalyzer(object):

    def __init__(self, database_name):

        self.dbname = database_name

    def fetch_top_three_articles(self):
        """Fetches a table containing the names of the top three articles and
           their hit count"""
        db = psycopg2.connect('dbname=' + self.dbname)
        cur = db.cursor()
        top_three_art_query = "select a.title, count(*) from log l join " \
                              "articles a on concat('/article/', a.slug) = " \
                              "l.path group by a.title order by count DESC " \
                              "limit 3;"
        cur.execute(top_three_art_query)
        rows = cur.fetchall()
        return rows

    def print_top_three_articles(self, rows):
        """Prints"""
        print("The top three most popular articles of all time are: ")
        print("1. {} - {} views".format(rows[0][0], rows[0][1]))
        print("2. {} - {} views".format(rows[1][0], rows[1][1]))
        print("3. {} - {} views".format(rows[2][0], rows[2][1]))
        print("\n")

    def fetch_top_authors(self):
        db = psycopg2.connect('dbname=' + self.dbname)
        cur = db.cursor()
        top_three_art_query = "select au.name, count(*) from log l join " \
                              "articles a on concat('/article/', a.slug) " \
                              "= l.path join authors au on au.id = a.author " \
                              "group by au.name order by count desc"
        cur.execute(top_three_art_query)
        rows = cur.fetchall()
        return rows

    def print_top_authors(self, rows):
        c = 1
        print("The most popular article authors of all time are: ")
        for row in rows:
            print("{}. {} - {} views".format(c, row[0], row[1]))
            c += 1
        print("\n")

    def fetch_high_error_days(self):
        db = psycopg2.connect('dbname=' + self.dbname)
        cur = db.cursor()
        high_error_days_query = "select t.day, cast((cast(error_count as " \
                                "decimal) / total_count*100) as " \
                                "numeric(3, 1))" \
                                " as error_percentage from " \
                                "(select to_char(time, 'Mon DD, YYYY') as " \
                                "day, " \
                                "count(status) as total_count from log " \
                                "group by day) t join (select to_char(" \
                                "time, 'Mon DD, YYYY') as day, " \
                                "count(status) " \
                                "as error_count from log where " \
                                "status != '200 OK' group by day) e on " \
                                "t.day = e.day where (cast(error_count as " \
                                "decimal) / total_count*100) > 1.0"
        cur.execute(high_error_days_query)
        rows = cur.fetchall()
        return rows

    def print_high_error_days(self, rows):
        print("The days with more than a 1% error rate are: ")
        for row in rows:
            print
            print("{} - {}% errors".format(row[0], row[1]))


if __name__ == '__main__':
    la = LogsAnalyzer(database_name='news')
    la.print_top_three_articles(rows=la.fetch_top_three_articles())
    la.print_top_authors(rows=la.fetch_top_authors())
    la.print_high_error_days(rows=la.fetch_high_error_days())
