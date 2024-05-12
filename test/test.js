import { test } from 'uvu';
import * as assert from 'uvu/assert';
import runQuery from '../index.cjs';
import { batchedAsyncGeneratorToArray, TypeFidelity } from '@evidence-dev/db-commons';

test('query runs', async () => {
	try {
		const { rows: row_generator, columnTypes } = await runQuery(
			"select 100 as number_col, sysdate as date_col, current_timestamp as timestamp_col, 'Evidence' as string_col FROM DUAL",
			{
				user: process.env.NODE_ORACLEDB_USER,
				password: process.env.NODE_ORACLEDB_PASSWORD,
				connectString: process.env.NODE_ORACLEDB_CONNECTIONSTRING,
				externalAuth: process.env.NODE_ORACLEDB_EXTERNALAUTH ? true : false
			}
		);
		const rows = await batchedAsyncGeneratorToArray(row_generator);
		assert.instance(rows, Array);
		assert.instance(columnTypes, Array);
		assert.type(rows[0], 'object');
		assert.equal(rows[0].number_col, 100);

		let actualColumnTypes = columnTypes.map((columnType) => columnType.evidenceType);
		let actualColumnNames = columnTypes.map((columnType) => columnType.name);
		let actualTypePrecisions = columnTypes.map((columnType) => columnType.typeFidelity);

		let expectedColumnTypes = ['number', 'date', 'date', 'string' /*, 'boolean' */];
		let expectedColumnNames = ['number_col', 'date_col', 'timestamp_col', 'string_col' /*, 'bool_col'*/];
		let expectedTypePrecision = Array(/*5*/4).fill(TypeFidelity.PRECISE);

		assert.equal(
			true,
			expectedColumnTypes.length === actualColumnTypes.length &&
				expectedColumnTypes.every((value, index) => value === actualColumnTypes[index])
		);
		assert.equal(
			true,
			expectedColumnNames.length === actualColumnNames.length &&
				expectedColumnNames.every((value, index) => value === actualColumnNames[index])
		);
		assert.equal(
			true,
			expectedTypePrecision.length === actualTypePrecisions.length &&
				expectedTypePrecision.every((value, index) => value === actualTypePrecisions[index])
		);
	} catch (e) {
		throw Error(e);
	}
});

test('query batches results properly', async () => {
	try {
		const { rows, expectedRowCount } = await runQuery(
			'select 1 FROM DUAL union all select 2 FROM DUAL union all select 3 FROM DUAL union all select 4 FROM DUAL union all select 5 FROM DUAL',
			{
				user: process.env.NODE_ORACLEDB_USER,
				password: process.env.NODE_ORACLEDB_PASSWORD,
				connectString: process.env.NODE_ORACLEDB_CONNECTIONSTRING,
				externalAuth: process.env.NODE_ORACLEDB_EXTERNALAUTH ? true : false
			},
			2
		);

		const arr = [];
		for await (const batch of rows()) {
			arr.push(batch);
		}
		for (const batch of arr.slice(0, -1)) {
			assert.equal(batch.length, 2);
		}
		assert.equal(arr[arr.length - 1].length, 1);
		assert.equal(expectedRowCount, 5);
	} catch (e) {
		throw Error(e);
	}
});

test.run();
