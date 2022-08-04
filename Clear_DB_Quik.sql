use Quik_dsp_yesterday
go
IF OBJECT_ID('tempdb.dbo.#table_name', 'U') IS NOT NULL DROP TABLE #table_name
IF OBJECT_ID('tempdb.dbo.#Count_Table', 'U') IS NOT NULL DROP TABLE #Count_Table
IF OBJECT_ID('tempdb.dbo.#Count_Table_2', 'U') IS NOT NULL DROP TABLE #Count_Table_2
--Создаем временную таблицу с названиями таблиц и колонок со временем
Create table #table_name (id int,Table_name varchar(max),Column_name varchar(max))
insert into #table_name Values
(1,'.dbo.trans_data','SERVER_TIME'),
(2,'.dbo.trans','SERVER_TIME'),
(3,'.dbo.trans_field_desc','trade_date'),
(4,'.dbo.trans_desc','trade_date'),
(5,'.dbo.depo_limit','set_time'),
(6,'.dbo.money_limit','set_time'),
(7,'.dbo.trade_correction','correction_time'),
(8,'.dbo.depo_limit_correction','set_time'),
(9,'.dbo.money_limit_correction ','set_time'),
(10,'.audit.AUDIT_LOG','action_time'),
(11,'.dbo.mssg','send_time'),
(12,'.dbo.QUIK_2AUTH_USERS_CFG_INFO','lastused'),
(13,'.dbo.QUIK_2AUTH_USERS_CFG_HISTORY','actiontime'),
(14,'.dbo.stop_orders','set_time')
select * from #table_name
----------------------------------------------------------------------------------------------------
declare @Count_table int				--создаем переменную для подсчета количества таблиц
declare @Table_name as varchar(max)     --переменная для хранения имени таблицы
declare @Column_name as varchar(max)    --переменная для хранения имени колонки таблицы со времененм
declare @RowNumber INT					--переменная для цикла
declare @dt_end as varchar(max)			--переменна для даты, до которой чистим данные
declare @dt_end_ as date='2018-03-01'	--переменная для проверки количества строк до и после очистки данных
----------------------------------------------------------------------------------------------------
--*задаем значение переменным
select @dt_end  ='''2018-03-01'''				--указываем дату, до которой чистим данные
select @RowNumber=2								--с какой таблицы из временной таблицы "#table_name" начинаем цикл
select @Count_table=count(*) from #table_name   --количество строк для обработки
--select @count_table
----------------------------------------------------------------------------------------------------
--Проверка количества строк до очистки данных и после
--Создаем временную таблицу с названиями таблиц и колонок со временем
Create table #Count_Table (id int,Table_Name varchar(max),Count_Before int,Min_Date_Before datetime)
insert into #Count_Table  values
(1,(select '.dbo.trans_data'),(select count(*) from .dbo.trans_data where   TRANS_ID in (select TRANS_ID from trans where server_time>=@dt_end_)),(select min(server_time) from .dbo.trans)),
(2,(select '.dbo.trans'),(select count(*) from .dbo.trans where server_time>=@dt_end_),(select min(server_time) from .dbo.trans)),
(3,(select '.dbo.trans_field_desc'),(select count(*) from .dbo.trans_field_desc where trade_date>=@dt_end_),(select min(trade_date) from .dbo.trans_field_desc)),
(4,(select '.dbo.trans_desc'),(select count(*) from .dbo.trans_desc where trade_date>=@dt_end_),(select min(trade_date) from .dbo.trans_desc)),
(5,(select '.dbo.depo_limit'),(select count(*) from .dbo.depo_limit where set_time>=@dt_end_),(select min(set_time) from .dbo.depo_limit)),
(6,(select '.dbo.money_limit'),(select count(*) from .dbo.money_limit where set_time>=@dt_end_),(select min(set_time) from .dbo.money_limit)),
(7,(select '.dbo.trade_correction'),(select count(*) from .dbo.trade_correction where correction_time>=@dt_end_),(select min(correction_time) from .dbo.trade_correction)),
(8,(select '.dbo.depo_limit_correction'),(select count(*) from .dbo.depo_limit_correction where set_time>=@dt_end_),(select min(set_time) from .dbo.depo_limit_correction)),
(9,(select '.dbo.money_limit_correction'),(select count(*) from .dbo.money_limit_correction where set_time>=@dt_end_),(select min(set_time) from .dbo.money_limit_correction)),
(10,(select '.audit.AUDIT_LOG'),(select count(*) from .audit.AUDIT_LOG  where action_time>=@dt_end_), (select min(action_time) from .audit.AUDIT_LOG)),
(11,(select '.dbo.mssg'),(select count(*) from .dbo.mssg  where send_time>=@dt_end_), (select min(send_time) from .dbo.mssg)),
(12,(select '.dbo.QUIK_2AUTH_USERS_CFG_INFO'),(select count(*) from .dbo.QUIK_2AUTH_USERS_CFG_INFO where lastused>=@dt_end_),(select min(lastused) from .dbo.QUIK_2AUTH_USERS_CFG_INFO)),
(13,(select '.dbo.QUIK_2AUTH_USERS_CFG_HISTORY'),(select count(*) from .dbo.QUIK_2AUTH_USERS_CFG_HISTORY where actiontime>=@dt_end_),(select min(actiontime) from .dbo.QUIK_2AUTH_USERS_CFG_HISTORY)),
(14,(select '.dbo.stop_orders'),(select count(*) from .dbo.stop_orders where set_time>=@dt_end_),(select min(set_time) from .dbo.stop_orders))


select * from #Count_Table
----------------------------------------------------------------------------------------------------
--1. Чистим данные из таблицы ".dbo.trans_data" в связи с разным кодом запроса
		select @Table_name=(select Table_name from #table_name where id=1)
		select @Table_name
		select @Column_name=(select Column_name from #table_name where id=1)
		select @Column_name
exec(
'
--1. Объявляем переменные
declare @logic_db_name as varchar(max) --Логическое имя файла db
declare @logic_log_name as varchar(max) --Логическое имя файла log
declare @dt_clear as date
declare @dt_last as date
declare @dt_end as varchar(max)
--3. Задаем значения в переменные
select @logic_db_name=(SELECT name FROM sys.database_files where type=0)
select @logic_log_name=(SELECT name FROM sys.database_files where type=1)
select @dt_end=' + @dt_end + '
select @dt_last=(select min(cast(server_time as date)) FROM trans)
--select @dt_clear=DATEADD(MONTH,1,@dt_last) --сколько месяцев чистим за раз (один месяц в данном примере)
select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
--4. Выводим данные для проверки
--select @dt_end as ''@dt_end дата до которой чистим данные''
--select @dt_last as ''@dt_last минимальная дата в таблице''
--select @dt_clear as ''@dt_clear дата разовой очистки''
--select @logic_db_name AS ''Логическое имя файла db''
--select @logic_log_name AS ''Логическое имя файла log''
--5. Запускаем цикл очистки данных
	while @dt_last<@dt_end
		begin
			delete from ' + @Table_name + ' 
			where TRANS_ID in 
				(select TRANS_ID from .dbo.trans where  ' + @Column_name + ' < @dt_clear)
						DBCC SHRINKFILE (@logic_db_name , 0, TRUNCATEONLY)
						DBCC SHRINKFILE (@logic_log_name , 0)
			select @dt_last=DATEADD(MONTH,1,@dt_last)
			--select @dt_clear=DATEADD(MONTH,1,@dt_last)
			select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
		end
'
)
----------------------------------------------------------------------------------------------------
--2. Чистим данные со второй по предпоследнюю таблицу из временной таблицы "#table_name"
While @RowNumber<@Count_table
	begin
		select @Table_name=(select Table_name from #table_name where id=@RowNumber)
		select @Table_name
		select @Column_name=(select Column_name from #table_name where id=@RowNumber)
		select @Column_name

		exec(
			'
			--2. Объявляем переменные
			declare @logic_db_name as varchar(max) --Логическое имя файла db
			declare @logic_log_name as varchar(max) --Логическое имя файла log
			declare @dt_clear as date
			declare @dt_last as date
			declare @dt_end as date
			--3. Задаем значения в переменные
			select @logic_db_name=(SELECT name AS ''Логическое имя файла db''  FROM sys.database_files where type=0)
			select @logic_log_name=(SELECT name AS ''Логическое имя файла log''  FROM sys.database_files where type=1)
			select @dt_end='+@dt_end+'
			select @dt_last=(select min(cast('+ @Column_name +' as date)) FROM ' + @Table_name + ')
			--select @dt_clear=DATEADD(MONTH,1,@dt_last) --сколько месяцев чистим за раз (один месяц в данном примере)
			select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
			--4. Выводим данные для проверки
			--select @dt_end as ''дата до которой чистим данные''
			--select @dt_last as ''минимальная дата в таблице''
			--select @dt_clear as ''дата разовой очистки''
			--select @logic_db_name AS ''Логическое имя файла db''
			--select @logic_log_name AS ''Логическое имя файла log''
			--5. Запускаем цикл очистки данных
				while @dt_last<@dt_end
				begin
					delete from ' + @Table_name + ' where ' + @Column_name + ' < @dt_clear

						DBCC SHRINKFILE (@logic_db_name , 0, TRUNCATEONLY)
						DBCC SHRINKFILE (@logic_log_name , 0)

					select @dt_last=DATEADD(MONTH,1,@dt_last)
					--select @dt_clear=DATEADD(MONTH,1,@dt_last)
					select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
				end
			'
		)
		--Переходим к следующей таблице
		select @RowNumber=@RowNumber+1
	end
----------------------------------------------------------------------------------------------------
--3. Чистим данные из последней таблицы "Stop_orders" временной таблицы "#table_name"
select @Table_name=(select Table_name from #table_name where id=14)
select @Table_name
select @Column_name=(select Column_name from #table_name where id=14)
select @Column_name
exec(
	'
	--2. Объявляем переменные
	declare @logic_db_name as varchar(max) --Логическое имя файла db
	declare @logic_log_name as varchar(max) --Логическое имя файла log
	declare @dt_clear as date
	declare @dt_last as date
	declare @dt_end as date
	--3. Задаем значения в переменные
	select @logic_db_name=(SELECT name AS ''Логическое имя файла db''  FROM sys.database_files where type=0)
	select @logic_log_name=(SELECT name AS ''Логическое имя файла log''  FROM sys.database_files where type=1)
	select @dt_end='+@dt_end+'
	select @dt_last=(select min(cast('+ @Column_name +' as date)) FROM ' + @Table_name + ')
	--select @dt_clear=DATEADD(MONTH,1,@dt_last) --сколько месяцев чистим за раз (один месяц в данном примере)
	select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
	--4. Выводим данные для проверки
	--select @dt_end as ''дата до которой чистим данные''
	--select @dt_last as ''минимальная дата в таблице''
	--select @dt_clear as ''дата разовой очистки''
	--select @logic_db_name AS ''Логическое имя файла db''
	--select @logic_log_name AS ''Логическое имя файла log''
	--5. Запускаем цикл очистки данных
		while @dt_last<@dt_end
		begin
			delete from ' + @Table_name + ' where ' + @Column_name + ' < @dt_clear
			and stop_order_id not in
			  (select stop_order_id from stop_orders so, users u
			   where ( ( (so.status=''N'' or so.status=''F'' or so.status=''Z'' or so.status=''P'') and
						(so.expiry_date >= convert(datetime,convert(char(10),getdate(),102),102)
			   or so.expiry_date is null) ) or
					  so.kill_time >= convert(datetime,convert(char(10),getdate(),102),102) or
			   so.run_time  >= convert(datetime,convert(char(10),getdate(),102),102) )
					and
					  (so.user_id=u.user_id and u.disabled=''n'' and u.mode not like ''%%l%%'' and
					   u.end_date   > convert(datetime,convert(char(10),getdate(),102),102) and
					   u.begin_date <= convert(datetime,convert(char(10),getdate(),102),102) ))

				DBCC SHRINKFILE (@logic_db_name , 0, TRUNCATEONLY)
				DBCC SHRINKFILE (@logic_log_name , 0)

			select @dt_last=DATEADD(MONTH,1,@dt_last)
			--select @dt_clear=DATEADD(MONTH,1,@dt_last)
			select @dt_clear=dateadd(mm,(select DATEDIFF ( mm , @dt_end , @dt_last ))+1,@dt_end)
		end
	'
)
--создаем временную таблицу для записи количества строк после очистки данных
Create table #Count_Table_2 (id int,Count_After int, Min_Date_After datetime)
insert into #Count_Table_2  values
(1,(select count(*) from .dbo.trans_data where   TRANS_ID in (select TRANS_ID from trans where server_time>=@dt_end_)),(select min(server_time) from .dbo.trans)),
(2,(select count(*) from .dbo.trans where server_time>=@dt_end_),(select min(server_time) from .dbo.trans)),
(3,(select count(*) from .dbo.trans_field_desc where trade_date>=@dt_end_),(select min(trade_date) from .dbo.trans_field_desc)),
(4,(select count(*) from .dbo.trans_desc where trade_date>=@dt_end_),(select min(trade_date) from .dbo.trans_desc)),
(5,(select count(*) from .dbo.depo_limit where set_time>=@dt_end_),(select min(set_time) from .dbo.depo_limit)),
(6,(select count(*) from .dbo.money_limit where set_time>=@dt_end_),(select min(set_time) from .dbo.money_limit)),
(7,(select count(*) from .dbo.trade_correction where correction_time>=@dt_end_),(select min(correction_time) from .dbo.trade_correction)),
(8,(select count(*) from .dbo.depo_limit_correction where set_time>=@dt_end_),(select min(set_time) from .dbo.depo_limit_correction)),
(9,(select count(*) from .dbo.money_limit_correction where set_time>=@dt_end_),(select min(set_time) from .dbo.money_limit_correction)),
(10,(select count(*) from .audit.AUDIT_LOG  where action_time>=@dt_end_),(select min(action_time) from .audit.AUDIT_LOG)),
(11,(select count(*) from .dbo.mssg  where send_time>=@dt_end_),(select min(send_time) from .dbo.mssg)),
(12,(select count(*) from .dbo.QUIK_2AUTH_USERS_CFG_INFO where lastused>=@dt_end_),(select min(lastused) from .dbo.QUIK_2AUTH_USERS_CFG_INFO)),
(13,(select count(*) from .dbo.QUIK_2AUTH_USERS_CFG_HISTORY where actiontime>=@dt_end_),(select min(actiontime) from .dbo.QUIK_2AUTH_USERS_CFG_HISTORY)),
(14,(select count(*) from .dbo.stop_orders where set_time>=@dt_end_),(select min(set_time) from .dbo.stop_orders))


--сверяем данные в таблицах #Count_Table и #Count_Table_2 до и после очистки данных
select a.id,a.Table_Name,a.Count_Before,b.Count_After, a.Min_Date_Before,b.Min_Date_After from #Count_Table a
join #Count_Table_2 b on a.id=b.id
