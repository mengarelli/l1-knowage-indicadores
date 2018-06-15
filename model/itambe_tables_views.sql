-- VIEWS
------------------------------------------------------------
CREATE VIEW vw_metas_anual AS
SELECT DATE_PART('year', mp.data_base) AS ano, id_plataforma, AVG(mp.volume_caminhao) AS meta_volume_veiculo,
	AVG(mp.volume_km) AS meta_densidade,
	AVG(mp.real_litro) AS meta_custo,
	AVG(me.perc_cumprimento) AS meta_cumprimento,
	AVG(me.perc_aderencia) AS meta_aderencia
FROM meta_plataforma mp
INNER JOIN meta me ON mp.data_base = me.data_base
GROUP BY 1, 2;

CREATE VIEW vw_metas_indicadores_anual AS
SELECT mm.ano, mm.id_plataforma, AVG(mm.meta_volume_veiculo) AS meta_volume_veiculo,
	(SELECT CASE WHEN SUM(li.num_veiculos) = 0 THEN 0 ELSE SUM(li.volume_dia) / SUM(li.num_veiculos) END AS volume_veiculo
		FROM logistica_indicador li
		WHERE DATE_PART('year', li.data_base) = mm.ano
		AND id_plataforma = mm.id_plataforma
	),
	AVG(mm.meta_densidade) AS meta_densidade,
	(SELECT CASE WHEN SUM(li.km) = 0 THEN 0 ELSE SUM(li.volume) / SUM(li.km) END AS densidade
		FROM logistica_indicador li
		WHERE DATE_PART('year', li.data_base) = mm.ano
		AND id_plataforma = mm.id_plataforma
	),
	AVG(mm.meta_custo) AS meta_custo,
	(SELECT CASE WHEN SUM(li.km) = 0 THEN 0 ELSE SUM(li.custo_reais) / SUM(li.volume) END AS custo
		FROM logistica_indicador li
		WHERE DATE_PART('year', li.data_base) = mm.ano
		AND id_plataforma = mm.id_plataforma
	),
	AVG(mm.meta_cumprimento) AS meta_cumprimento,
	(SELECT AVG(pr.perc_cumprimento) AS cumprimento
		FROM perc_roteirizacao pr
		WHERE DATE_PART('year', pr.data_base) = mm.ano
		AND id_plataforma = mm.id_plataforma
		AND pr.perc_cumprimento <> 0
	),
	AVG(mm.meta_aderencia) AS meta_aderencia,
	(SELECT AVG(pr.perc_aderencia) AS aderencia
		FROM perc_roteirizacao pr
		WHERE DATE_PART('year', pr.data_base) = mm.ano
		AND id_plataforma = mm.id_plataforma
		AND pr.perc_aderencia <> 0
	)	
FROM vw_metas_anual mm
GROUP BY mm.ano, id_plataforma;


-- TABELAS DE DADOS
------------------------------------------------------------




CREATE TABLE meta (
                data_base DATE NOT NULL,
                perc_cumprimento NUMERIC(7,3) NOT NULL,
                perc_aderencia NUMERIC(7,3) NOT NULL,
                CONSTRAINT meta_pk PRIMARY KEY (data_base)
);


CREATE TABLE transportadora (
                id_transportadora INTEGER NOT NULL,
                nome_transportadora VARCHAR(200) NOT NULL,
                CONSTRAINT transportadora_pk PRIMARY KEY (id_transportadora)
);


CREATE TABLE de_para_trans (
                nome_de VARCHAR(200) NOT NULL,
                id_transportadora INTEGER NOT NULL,
                CONSTRAINT de_para_trans_pk PRIMARY KEY (nome_de)
);


CREATE TABLE plataforma (
                id_plataforma INTEGER NOT NULL,
                nome_plataforma VARCHAR(200) NOT NULL,
                CONSTRAINT plataforma_pk PRIMARY KEY (id_plataforma)
);


CREATE TABLE meta_plataforma (
                data_base DATE NOT NULL,
                id_plataforma INTEGER NOT NULL,
                real_litro NUMERIC(7,4) NOT NULL,
                volume NUMERIC(12) NOT NULL,
                km NUMERIC(12) NOT NULL,
                custo_reais NUMERIC(12,2) NOT NULL,
                num_veiculos NUMERIC(7,2) NOT NULL,
                volume_km NUMERIC(7,4) NOT NULL,
                volume_caminhao NUMERIC(12,4) NOT NULL,
                CONSTRAINT meta_plataforma_pk PRIMARY KEY (data_base, id_plataforma)
);


CREATE SEQUENCE logistica_indicador_id_indicador_seq;

CREATE TABLE logistica_indicador (
                id_indicador INTEGER NOT NULL DEFAULT nextval('logistica_indicador_id_indicador_seq'),
                data_base DATE NOT NULL,
                id_plataforma INTEGER NOT NULL,
                cod_pagamento VARCHAR NOT NULL,
                local_entrega VARCHAR(200) NOT NULL,
                custo_reais NUMERIC(12,2) NOT NULL,
                volume NUMERIC(12) NOT NULL,
                volume_dia NUMERIC(12,4) NOT NULL,
                km NUMERIC(12) NOT NULL,
                num_veiculos NUMERIC(7,2) NOT NULL,
                CONSTRAINT logistica_indicador_pk PRIMARY KEY (id_indicador)
);


ALTER SEQUENCE logistica_indicador_id_indicador_seq OWNED BY logistica_indicador.id_indicador;

CREATE TABLE de_para_plata (
                nome_de VARCHAR(200) NOT NULL,
                id_plataforma INTEGER NOT NULL,
                CONSTRAINT de_para_plata_pk PRIMARY KEY (nome_de)
);


CREATE TABLE perc_roteirizacao (
                data_base DATE NOT NULL,
                id_plataforma INTEGER NOT NULL,
                perc_cumprimento NUMERIC(7,3) NOT NULL,
                perc_aderencia NUMERIC(7,3) NOT NULL,
                CONSTRAINT perc_roteirizacao_pk PRIMARY KEY (data_base, id_plataforma)
);


ALTER TABLE de_para_trans ADD CONSTRAINT transportadora_de_para_trans_fk
FOREIGN KEY (id_transportadora)
REFERENCES transportadora (id_transportadora)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE perc_roteirizacao ADD CONSTRAINT plataforma_perc_roteirizacao_fk
FOREIGN KEY (id_plataforma)
REFERENCES plataforma (id_plataforma)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE de_para_plata ADD CONSTRAINT plataforma_de_para_plata_fk
FOREIGN KEY (id_plataforma)
REFERENCES plataforma (id_plataforma)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE logistica_indicador ADD CONSTRAINT plataforma_logistica_indicadores_fk
FOREIGN KEY (id_plataforma)
REFERENCES plataforma (id_plataforma)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE meta_plataforma ADD CONSTRAINT plataforma_metas_fk
FOREIGN KEY (id_plataforma)
REFERENCES plataforma (id_plataforma)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;
