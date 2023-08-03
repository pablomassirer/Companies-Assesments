-- Mr.Health Case Challenge --

-- PRODUTO Table

create table if not exists produto 
(
	Id_produto Int primary key not null,
	Nome_Produto VARCHAR(30) not null
);

-- UNIDADE Table

 create table if not exists unidade
 (
	Id_Unidade Int primary key not null,
	Nome_Unidade VARCHAR(30),
	Id_Estado Int references estado
 );
 
 -- ESTADO Table

 create table if not exists estado
 (
	Id_Pais INT primary key no null,
	Nome_Pais VARCHAR(30)
 );
 
 -- PAIS Table

 create table if not exists pais
 (
	Id_Estado Int primary key not null,
	Id_Pais INT references pais,
	Nome_Estado VARCHAR(30)
 );

-- PEDIDO Table

create table if not exists pedido (
	Id_Unidade INT not null references unidade,
	Id_Pedido INT primary key not null,
	Tipo_Pedido SMALLINT,
	Data_Pedido DATE,
	Vlr_Pedido real not null,
	Endereco_Entrega VARCHAR(50),
	Taxa_Entrega REAL,
	Status SMALLINT not null
);

comment on column pedido.Tipo_Pedido is 'Valor 1 para Loja Online e 2 para Loja Física.';
comment on column pedido.Status is 'Valor 1 para Pendente; 2 para Finalizado e 3 para Cancelado.';

-- ITEM_PEDIDO Table

create table if not exists item_pedido (
	Id_Pedido INT not null references pedido,
	Id_Item_Pedido INT primary key not null,
	Id_Produto INT not null references produto,
	Qtd smallint not null,
	Vlr_Item  real not null,
	Observacao Varchar(60)
);



