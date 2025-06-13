Create database ListaDeExercicios_2 --Já existe um bd com esse nome, então vou colocar um underline pra diferenciar :)
go

use ListaDeExercicios_2
go

--//////////////////PARTE 1///////////////////////

--Alterando o modo de recuperação para BUlk_LOgged
alter database ListaDeExercicios_2
set recovery Bulk_Logged
go

--Dados já exportados, agora definir uma coluna chaveprimaria autonumerada
alter table [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
add ColunaChavePrimaria int identity(1,1)
go

--coluna criada, agora definir como primary key
alter table [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
add constraint PK_ColunaCHavePrimaria primary  key (ColunaChavePrimaria)
go

--alterando o tipo de dado da coluna DAtaHora
alter table [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
alter column DataHora DateTime
go

--novo indice nonclusteres para coluna DataHora
create Nonclustered index IX_DataHora
on [Banco de Dados II – Lista de Exercícios II – Queimadas 2023](DataHora)
go

--novo indice nonclustered para a coluna Bioma
create NonClustered index IX_Bioma
on [Banco de Dados II – Lista de Exercícios II – Queimadas 2023](Bioma)
go

--criação de uma nova estatístiva para a coluna DiaSemChuva
create statistics StatisticsDiaSemChuva
on [Banco de Dados II – Lista de Exercícios II – Queimadas 2023](DiaSemChuva)
go

--varificando se a statistics foi criada certinho
DBCC show_statistics ('[Banco de Dados II – Lista de Exercícios II – Queimadas 2023]', StatisticsDiaSemChuva)
go

--//////////////////PARTE 2/////////////////////////

--criando uma view 
create view V_VisaoMesEstadoMunicipioBioma as 
	select MONTH(DataHora) as Mes,
		Estado,
		Municipio,
		Bioma,
		COUNT(*) as TotalDeQueimadas
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where MONTH(DataHora) in (2,4,6,8,10,12)
group by
		MONTH(DataHora), Estado, Municipio, Bioma
go

--verificando rapidamenta a view
select * from V_VisaoMesEstadoMunicipioBioma
go

--criando a view  V_DuzentasPrimeirasQueimadas
create view V_DuzentasPrimeirasQueimadas as
	select
		Estado,
		Municipio,
		Bioma,
		Convert(varchar, FIRST_VALUE(DataHora) over (Partition by Estado, Municipio, Bioma order by DataHora), 103) as [Primeira Queimada Ocorrida],
		Convert(varchar, LAST_VALUE(DataHora) over (Partition by Estado, Municipio, Bioma order by DataHora rows between unbounded preceding and unbounded following), 103) as [Última Queimada Ocorrida]
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
go

--verificandp rapidamente a view
select * from V_DuzentasPrimeirasQueimadas
go

--fazendo a CTE
With CTERanqueamento as (
	Select	
		row_number() over (order by Estado, Municipio, Bioma) as Ranking,
		Estado,
		Municipio,
		Bioma
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
)
select*from CTERanqueamento
go

--adicionando uma nova coluna que computa a quantidade de queimadas
With CTERanqueamento as (
	select 
		ROW_NUMBER() over (order by Estado, Municipio, Bioma) as Ranking,
		Estado,
		Municipio,
		Bioma,
		(Select COUNT(*) from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]) as Quantidade
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
)
select*from CTERanqueamento
go

--modificando a partição de ranqueamento de dados 
with CTERanqueamento as(
	select
		ROW_NUMBER() over (partition by count(DataHora) order by Estado, Municipio, Bioma) as Ranking,
		Estado,
		Municipio,
		Bioma,
		Count(*) as Quantidade 
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
	group by Estado, Municipio, Bioma
)
Select Ranking, Estado, Municipio, Bioma, Quantidade
from CTERanqueamento
go

--///////////////PARTE 3/////////////////////

--Criando a table HistpricoQueimadas2023
create table HistoricoQueimadas2023(
	DataHora datetime not null,
	Satelite nvarchar(50) not null,
	Pais nvarchar(50) not null,
	Estado nvarchar(50) not null,
	Municipio nvarchar(50) not null,
	Bioma nvarchar(50) not null,
	DiaSemChuva tinyint not null,
	Precipitacao float not null,
	RiscoFogo float not null,
	Latitude float not null,
	Manipulacao nvarchar(50) not null
)
go

--armazenando os registros da table queimadas2023
insert into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
select top 10 DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'UPDATE'
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where CONVERT(varchar, DataHora, 23) = '2023-05-14';
go

--UPDATE dos registros na table queiamdas2023
update top (10) [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
set Estado = 'NovoEstado', Municipio = 'NovoMunicipio'
where CONVERT(varchar, DataHora, 23) = '2023-05-14';
go

--verificando os dados de HIstorico
select * from HistoricoQueimadas2023
where Manipulacao = 'UPDATE'
go

--verificando se os registros foram atualizados na table Queimadas
Select * from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where CONVERT(varchar, DataHora, 23) = '2023-05-14'
go

--Insere os registros que serão deletados da tabela
insert into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
select top 10 DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'DELETE'
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where Estado = 'Minas Gerais'
go

--DELETE dos registros
delete top (10)
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where Estado = 'Minas Gerais'
go

--verificando os dados de Historico
select * from HistoricoQueimadas2023
where Manipulacao = 'DELETE'
go

--verificando os registros que sobraram de minas gerais na table Queimadas
Select * from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where Estado = 'Minas Gerais'
go

--Inserindo os dados na table QUeimadas2023
Insert into [Banco de Dados II – Lista de Exercícios II – Queimadas 2023] 
(DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude)
Values
	('2023-06-01T12:00:00', 'SateliteA', 'Brasil', 'EstadoA', 'MunicipioA', 'BiomaA', 10, 5.0, 0.1, -15.0),
	('2023-06-02T12:00:00', 'SateliteB', 'Brasil', 'EstadoB', 'MunicipioB', 'BiomaB', 12, 4.5, 0.2, -14.0),
	('2023-06-03T12:00:00', 'SateliteC', 'Brasil', 'EstadoC', 'MunicipioC', 'BiomaC', 15, 6.0, 0.3, -13.0),
	('2023-06-04T12:00:00', 'SateliteD', 'Brasil', 'EstadoD', 'MunicipioD', 'BiomaD', 8, 3.5, 0.4, -12.0),
	('2023-06-05T12:00:00', 'SateliteE', 'Brasil', 'EstadoE', 'MunicipioE', 'BiomaE', 20, 2.5, 0.5, -11.0),
	('2023-06-06T12:00:00', 'SateliteF', 'Brasil', 'EstadoF', 'MunicipioF', 'BiomaF', 25, 7.5, 0.6, -10.0),
	('2023-06-07T12:00:00', 'SateliteG', 'Brasil', 'EstadoG', 'MunicipioG', 'BiomaG', 18, 8.5, 0.7, -9.0),
	('2023-06-08T12:00:00', 'SateliteH', 'Brasil', 'EstadoH', 'MunicipioH', 'BiomaH', 5, 9.5, 0.8, -8.0),
	('2023-06-09T12:00:00', 'SateliteI', 'Brasil', 'EstadoI', 'MunicipioI', 'BiomaI', 30, 10.5, 0.9, -7.0),
	('2023-06-10T12:00:00', 'SateliteJ', 'Brasil', 'EstadoJ', 'MunicipioJ', 'BiomaJ', 35, 11.5, 1.0, -6.0),
	('2023-06-11T12:00:00', 'SateliteK', 'Brasil', 'EstadoK', 'MunicipioK', 'BiomaK', 7, 12.5, 1.1, -5.0),
	('2023-06-12T12:00:00', 'SateliteL', 'Brasil', 'EstadoL', 'MunicipioL', 'BiomaL', 13, 13.5, 1.2, -4.0),
	('2023-06-13T12:00:00', 'SateliteM', 'Brasil', 'EstadoM', 'MunicipioM', 'BiomaM', 16, 14.5, 1.3, -3.0),
	('2023-06-14T12:00:00', 'SateliteN', 'Brasil', 'EstadoN', 'MunicipioN', 'BiomaN', 22, 15.5, 1.4, -2.0),
	('2023-06-15T12:00:00', 'SateliteO', 'Brasil', 'EstadoO', 'MunicipioO', 'BiomaO', 9, 16.5, 1.5, -1.0),
	('2023-06-16T12:00:00', 'SateliteP', 'Brasil', 'EstadoP', 'MunicipioP', 'BiomaP', 11, 17.5, 1.6, 0.0),
	('2023-06-17T12:00:00', 'SateliteQ', 'Brasil', 'EstadoQ', 'MunicipioQ', 'BiomaQ', 14, 18.5, 1.7, 1.0),
	('2023-06-18T12:00:00', 'SateliteR', 'Brasil', 'EstadoR', 'MunicipioR', 'BiomaR', 17, 19.5, 1.8, 2.0),
	('2023-06-19T12:00:00', 'SateliteS', 'Brasil', 'EstadoS', 'MunicipioS', 'BiomaS', 21, 20.5, 1.9, 3.0),
	('2023-06-20T12:00:00', 'SateliteT', 'Brasil', 'EstadoT', 'MunicipioT', 'BiomaT', 19, 21.5, 2.0, 4.0);
go

--INSERT dos dados na table HistoricoQUeimadas2023
insert into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
select 
    DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'INSERT'
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where DataHora BETWEEN '2023-06-01T12:00:00' AND '2023-06-20T12:00:00';
go

--verificando os dados inseridos na table Historico
Select * from HistoricoQueimadas2023
where Manipulacao = 'INSERT'
go

--Verifica o INSERT de dados na table QUeimadas2023
Select * from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
where DataHora between '2023-06-01T12:00:00' AND '2023-06-20T12:00:00'
go

--///////////////////////PARTE 4/////////////////////////////

--Criando a procedure para filtrar pelo mes
Create Procedure P_FiltrarMesesQueimadas
	@Mes int
as
begin
	set nocount on

	select DataHora, Estado, Municipio, Latitude
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
	where Month(DataHora) = @Mes
	order by DataHora asc
end
go

--Consultando
exec P_FiltrarMesesQueimadas @Mes = 3
go

--Criando store procedure para filtrar pelo local
Create procedure P_FiltrarLocalQueimada
	@Local nvarchar(50),
	@TipoLocal nvarchar(50)
as
begin
	set nocount on

	if @TipoLocal = 'Estado'
	begin
		Select Estado as NomeLocal, Municipio, Bioma, DataHora
		from  [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
		Where Estado = @Local
		order by DataHora asc
	end
	else if @TipoLocal = 'Municipio'
	begin
		select Municipio as NomeLocal, DataHora, Bioma, Estado
		from  [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
		where Municipio = @Local
		order by DataHora asc
	end
	else -- vou colcoar uma condição para retornar mensagem de erro em caso de digitar algo diferente de Estado e Municipio
	begin
		Raiserror('Tipo de local inválido. Use "Estado" ou "Municipio" >:(', 16, 1)
		--Severity 16 = erro gerado pelo user
	end
end
go

--executar filtrando por ESTADO
exec P_FiltrarLocalQueimada @Local = 'BAHIA', @TipoLocal = 'Estado'
go

--executar filtrando por MUNICIPIO
exec P_FiltrarLocalQueimada @Local = 'Coari', @TipoLocal = 'Muicipio'
go --deixei Municipio escrito errado para testar se o comando raiserror funcionou

--Nessa tarefa está pedindo a longitude, mas não tem nos dados, apenas latitude, 
--então vou usar latitude como parametro
Create Function F_PesquisarLatitude(
	@Latitude float
)
returns table
as
Return(
	select Municipio, Estado
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]	
	where Latitude = @Latitude
)
go

--consultando os municipios com a latitude desejada
select * from F_PesquisarLatitude(-1200492)
go

--Função para pesquisar por bioma
alter Function F_PesquisarBioma(
	@Local nvarchar(50),
	@TipoLocal nvarchar(50)
)
returns table
as
return(
	Select Bioma, Estado, Municipio
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
	where(@TipoLocal = 'Estado' and Estado = @Local)
	or (@TipoLocal = 'Municipio' and Municipio = @Local)
)
go

select * from F_PesquisarBioma('BAHIA', 'ESTADO')
go

--/////////////////PARTE 5//////////////////////

--adicionando valores padroes na procedure P_FiltrarMesesQueimadas 
Alter Procedure P_FiltrarMesesQueimadas
	@Mes int = null
as
begin
	set nocount on
	If @Mes is null --Se não definir um mes na execução da procedure, irá utilizzar o mes atual
		Set @Mes = MONTH(GETDATE())
	select DataHora, Estado, Municipio, Latitude
	from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
	where Month(DataHora) = @Mes
	order by DataHora asc
end
go

-- executando procedure
exec P_FiltrarMesesQueimadas
go

--Adicionando valores padroes na procedure P_FiltrarLocalQueimada
Alter procedure P_FiltrarLocalQueimada
	@Local nvarchar(50) = null,
	@TipoLocal nvarchar(50) = null
as
begin
	set nocount on

	if @TipoLocal = '' and @Local = ''
	begin-- Defini Estado e São Paulo como Padrão caso a variavel seja declarada nula, e se digitar algo errado retorna o erro
		set @TipoLocal = 'Estado' 
		set @Local = 'São Paulo'
	end

	if @Local = ''
	begin --mostra mensagem de erro caso Preencah
		raiserror ('Por favor, preencha @Local com nome de alguma cidade ou estado', 16, 1)
	end

	if @TipoLocal = 'Estado'
	begin
		Select Estado as NomeLocal, Municipio, Bioma, DataHora
		from  [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
		Where Estado = @Local
		order by DataHora asc
	end

	else if @TipoLocal = 'Municipio'
	begin
		select Municipio as NomeLocal, DataHora, Bioma, Estado
		from  [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
		where Municipio = @Local
		order by DataHora asc
	end

	else 
	begin-- vou colcoar uma condição para retornar mensagem de erro em caso de digitar algo diferente de Estado e Municipio
		Raiserror('Tipo de local inválido. Use "Estado" ou "Municipio"', 16, 1)
		--Severity 16 = erro gerado pelo user
	end
end
go

--executando a procedure com São Paulo e Estado como Padrão
exec P_FiltrarLocalQueimada @Local = '', @TipoLocal = ''
go

--testando para ver se o erro ainda está funcionando (um pouco contraditório)
exec P_FiltrarLocalQueimada @Local = '', @TipoLocal = 'Municpio'
go --erros funionando corretamente

--diretivas
set arithabort on -- aborte a execução e não apresente erros matemáticos
set ansi_warnings on -- não exiba warnings
set NoCount on -- Não apresenta a contagem de linhas afetadas
set Language Brazilian

begin try -- abrindo o bloco do codigo protegido
 select 10/0
end try
begin catch -- abrindo o bloco de codigo para tratamento e apresentação
 select ERROR_LINE() as 'linha', ERROR_MESSAGE() as 'mensagem',
		ERROR_NUMBER() as 'numero', ERROR_SEVERITY() as 'severity',
		ERROR_STATE() as 'state', ERROR_PROCEDURE() as 'procedure'
end catch
go

--Criando uma view do codigo fonte
create view dbo.QueimadasEncryption
with encryption
as
select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude
from [Banco de Dados II – Lista de Exercícios II – Queimadas 2023]
go

--verificando a criação da view
select *
from sys.views where name = 'QueimadasEncryption'
go

-- consultando os dados da view
select * 
from QueimadasEncryption
go

--tentando visualizar a definição da view
exec sp_helptext 'QueimadasEncryption'

go