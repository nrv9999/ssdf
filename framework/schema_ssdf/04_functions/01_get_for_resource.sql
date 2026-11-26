DROP FUNCTION IF EXISTS ssdf.get_for_resource(int);

create or replace function ssdf.get_for_resource(in _id int) returns bool
as $$
begin
return true;
end
$$ LANGUAGE plpgsql;
