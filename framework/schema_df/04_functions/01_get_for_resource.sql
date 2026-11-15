DROP FUNCTION IF EXISTS df.get_for_resource(int);

create or replace function df.get_for_resource(in _id int) returns bool
as $$
begin
return true;
end
$$ LANGUAGE plpgsql;
