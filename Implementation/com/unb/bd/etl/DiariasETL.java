package com.unb.bd.etl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

public class DiariasETL {
	private static final String CsvFile = "201407_Diarias.csv";

	private static DiariasETL instance = null;

	private BufferedReader csvReader;
	private DataBase dataBase;

	private DiariasETL() {
		dataBase = DataBase.getInstance();

		try {
			csvReader = new BufferedReader(new FileReader(CsvFile));
		} catch (FileNotFoundException e) {
			System.out.println("Nao foi possivel abrir o arquivo csv.");
			e.printStackTrace();
		}
	}

	public static DiariasETL getInstance() {
		if (instance == null) {
			return new DiariasETL();
		} else {
			return instance;
		}
	}

	public void execute() {
		try {
			// Skip columns name line
			csvReader.readLine();
			String lineRead;
			while ((lineRead = csvReader.readLine()) != null) {
				String[] fields = lineRead.split(";|\t");

				if (fields.length > 21) {
					System.out.println("Algum erro ocorreu ao ler linha atual."
							+ "\n Continunado a partir da proxima.");
					continue;
				}

				insertLineOfFields(fields);

			}

		} catch (IOException e) {
			System.out.println("Nao foi possivel ler o arquivo csv.");
			e.printStackTrace();
		}
	}

	private void insertLineOfFields(String[] fields) {
		// Orgao
		insert("TB_orgao",
				new String[] { fields[0],
						DataBase.formatStringToBank(fields[1]), "NULL" });

		// Suborgao
		insert("TB_orgao",
				new String[] { fields[2],
						DataBase.formatStringToBank(fields[3]), fields[0] });

		// unidade

		insert("TB_unidade",
				new String[] { fields[4],
						DataBase.formatStringToBank(fields[5]) });

		// funcao

		insert("TB_funcao",
				new String[] { fields[6],
						DataBase.formatStringToBank(fields[7]), "NULL" });

		// sub funcao

		insert("TB_funcao",
				new String[] { fields[8],
						DataBase.formatStringToBank(fields[9]), fields[6] });

		// programa

		insert("TB_programa",
				new String[] { fields[10],
						DataBase.formatStringToBank(fields[11]) });

		// acao

		insert("TB_acao",
				new String[] { DataBase.formatStringToBank(fields[12]),
						DataBase.formatStringToBank(fields[13]),
						DataBase.formatStringToBank(fields[14]) });

		// favorecido

		insert("TB_favorecido",
				new String[] { DataBase.formatStringToBank(fields[15]),
						DataBase.formatStringToBank(fields[16]) });

		// pagamento

		/*
		 * fields[15] -> favorecido fields[4] -> unidade fields[8] -> subfunçao
		 * fields[2] -> suborgao fields[10] -> programa fields[12] -> acao
		 */

		insert("TB_pagamento",
				new String[] { DataBase.formatStringToBank(fields[17]),
						fields[18], DataBase.formatDateToBank(fields[19]),
						fields[20].replace(',', '.'),
						DataBase.formatStringToBank(fields[15]), fields[4],
						fields[8], fields[2], fields[10],
						DataBase.formatStringToBank(fields[12]) });
	}

	private void insert(String tableName, String[] values) {

		String query = "INSERT IGNORE INTO " + tableName + " VALUES(";

		for (String value : values) {
			query = query + value + ",";
		}

		query = query.substring(0, query.length() - 1) + ");";

		System.out.println(query);

		try {
			dataBase.executeSqlUpdateQuery(query);
		} catch (SQLException e) {
			System.out.println("Nao foi possivel executar a query.");
			e.printStackTrace();
		}
	}
}
