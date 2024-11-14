
select date, client_type, future_index_long, future_index_long from pwVOLUME
where client_type = 'FII';


CREATE TABLE pwvolume (
    Date DATE,
    Client_Type VARCHAR(50),
    Future_Index_Long INT,
    Future_Index_Short INT,
    Future_Stock_Long INT,
    Future_Stock_Short INT,
    Option_Index_Call_Long BIGINT,
    Option_Index_Put_Long BIGINT,
    Option_Index_Call_Short BIGINT,
    Option_Index_Put_Short BIGINT,
    Option_Stock_Call_Long INT,
    Option_Stock_Put_Long INT,
    Option_Stock_Call_Short INT,
    Option_Stock_Put_Short INT,
    Total_Long_Contracts BIGINT,
    Total_Short_Contracts BIGINT
);

create table pwoi(
	Date DATE,
    Client_Type VARCHAR(50),
    Future_Index_Long INT,
    Future_Index_Short INT,
    Future_Stock_Long INT,
    Future_Stock_Short INT,
    Option_Index_Call_Long BIGINT,
    Option_Index_Put_Long BIGINT,
    Option_Index_Call_Short BIGINT,
    Option_Index_Put_Short BIGINT,
    Option_Stock_Call_Long INT,
    Option_Stock_Put_Long INT,
    Option_Stock_Call_Short INT,
    Option_Stock_Put_Short INT,
    Total_Long_Contracts BIGINT,
    Total_Short_Contracts BIGINT
	
);

