movie_table_insert = """
        insert into movies (imdb_id, title, year, rating, runtime, mpa_rating, language, date_uploaded)
        VALUES (?,?,?,?,?,?,?,?)
        """

genre_moviegenre_insert = """
                        declare @temp_genre_id int;
                        if not exists (
                                        select * from genre
                                        where genre_title = ?
                                    )
                        begin
                            insert into genre values(?)
                        end
                select @temp_genre_id  = genre_id from genre where genre_title = ?
                insert into movie_genre
                values(?, @temp_genre_id)
            """

cast_moviecast_insert = """
                        declare @temp_actor_id int;
                        if not exists (
                                        select * from cast
                                        where actor_name = ?
                                    )
                        begin
                                insert into cast values(?)
                        end
                select @temp_actor_id  = actor_id from cast where actor_name = ?
                insert into movie_cast
                values(?, @temp_actor_id)
            """

summary_insert = """

            insert into summary (imdb_id, summary)
            values (?, ?)

            """
