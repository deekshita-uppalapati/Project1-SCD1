use project_1;

select * from day where custid not in (select custid from day_recol);
