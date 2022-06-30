insert into movies values ('tt0031381', 'Gone with the Wind (1939)');

declare @genre_title varchar(50) = 'Adventure'

if not exists (
select * from genres
where genre_title = @genre_title
)
begin
	insert into genres values(@genre_title)
end


use yts_warehouse

select *from movies
select *from genre

declare @gen_t varchar(30) = 'Action'
declare @imdb_c varchar(30) = 'tt0926084'
declare @temp_genre_id int;
if not exists (
                select * from genre
                where genre_title = @gen_t
                )

                begin
                insert into genre values(@genre_title)
               
                
                select @temp_genre_id  = genre_id from genre where genre_title = @gen_t
                insert into movie_genre
                values(@imdb_c, @temp_genre_id)
                end



