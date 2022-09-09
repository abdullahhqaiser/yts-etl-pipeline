movie_table_insert = """
        insert into movies (imdb_id, title, year, rating, runtime, mpa_rating, language, date_uploaded)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

genre_moviegenre_insert = """
                        declare @temp_genre_id int;
                        if not exists (
                                        select * from genre
                                        where genre_title = %s
                                    )
                        begin
                            insert into genre values(%s)
                        end
                select @temp_genre_id  = genre_id from genre where genre_title = %s
                insert into movie_genre
                values(%s, @temp_genre_id)
            """

cast_moviecast_insert = """
                        declare @temp_actor_id int;
                        if not exists (
                                        select * from cast
                                        where actor_name = %s
                                    )
                        begin
                                insert into cast values(%s)
                        end
                select @temp_actor_id  = actor_id from cast where actor_name = %s
                insert into movie_cast
                values(%s, @temp_actor_id)
            """

summary_insert = """

            insert into summary (imdb_id, summary)
            values (%s, %s)

            """
