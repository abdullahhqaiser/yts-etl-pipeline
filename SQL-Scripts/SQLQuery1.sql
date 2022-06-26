insert into movies values ('tt0031381', 'Gone with the Wind (1939)');

declare @genre_title varchar(50) = 'Adventure'

if not exists (
select * from genres
where genre_title = @genre_title
)
begin
	insert into genres values(@genre_title)
end


