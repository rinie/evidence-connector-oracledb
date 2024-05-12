const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator, // not in postgress but in mssql and mysql
	cleanQuery,
	exhaustStream
} = require('@evidence-dev/db-commons');
const oracledb = require('oracledb');

/**
 *
 * @param {(() => oracledb.ISqlType) | oracledb.ISqlType} data_type
 * @param {undefined} defaultType
 * @returns {EvidenceType | undefined}
 */
function nativeTypeToEvidenceType(data_type, defaultType = undefined) {
	switch (data_type) {
		case oracledb.DB_TYPE_BINARY_DOUBLE:
		case oracledb.DB_TYPE_BINARY_FLOAT:
		case oracledb.DB_TYPE_BINARY_INTEGER:
		case oracledb.DB_TYPE_NUMBER:
			return EvidenceType.NUMBER;

		case oracledb.DB_TYPE_DATE:
		case oracledb.DB_TYPE_INTERVAL_DS:
		case oracledb.DB_TYPE_INTERVAL_YM:
		case oracledb.DB_TYPE_TIMESTAMP:
		case oracledb.DB_TYPE_TIMESTAMP_LTZ:
		case oracledb.DB_TYPE_TIMESTAMP_TZ:
			return EvidenceType.DATE;

		case oracledb.DB_TYPE_CHAR:
		case oracledb.DB_TYPE_VARCHAR:
		case oracledb.DB_TYPE_XMLTYPE:
		case oracledb.DB_TYPE_JSON:
		case oracledb.DB_TYPE_NCHAR:
		case oracledb.DB_TYPE_NVARCHAR:
			return EvidenceType.STRING;

		case oracledb.DB_TYPE_BOOLEAN:
			return EvidenceType.BOOLEAN;

		case oracledb.DB_TYPE_BFILE:
		case oracledb.DB_TYPE_BLOB:
		case oracledb.DB_TYPE_CLOB:
		case oracledb.DB_TYPE_CURSOR:
		case oracledb.DB_TYPE_LONG:
		case oracledb.DB_TYPE_LONG_NVARCHAR:
		case oracledb.DB_TYPE_LONG_RAW:
		case oracledb.DB_TYPE_NCLOB:
		case oracledb.DB_TYPE_OBJECT:
		case oracledb.DB_TYPE_RAW:
		case oracledb.DB_TYPE_ROWID:
		case oracledb.DB_TYPE_UROWID:
		case oracledb.DB_TYPE_VECTOR:
		default:
			return defaultType;
	}
}

/**
 *
 * @param {oracledb.IColumnMetadata} fields
 * @returns
 */
const mapResultsToEvidenceColumnTypes = function (fields) {
	return Object.values(fields).map((field) => {
		/** @type {TypeFidelity} */
		let typeFidelity = TypeFidelity.PRECISE;
		let evidenceType = nativeTypeToEvidenceType(field.dbType);

		if (!evidenceType) {
			typeFidelity = TypeFidelity.INFERRED;
			evidenceType = EvidenceType.STRING;
		}
		return {
			name: field.name.toLowerCase(),
			evidenceType: evidenceType,
			typeFidelity: typeFidelity
		};
	});
};

/**
 *
 * @param {Record<string, unknown>[]} result
 * @returns {Record<string, unknown>[]}
 */
const standardizeResult = (result, columnTypes, outFormat) => {
	/** @type {Record<string, unknown>[]} */
	if (outFormat === oracledb.OUT_FORMAT_OBJECT) {
		const output = [];
		result.forEach((row) => {
			/** @type {Record<string, unknown>} */
			const lowerCasedRow = {};
			for (const [key, value] of Object.entries(row)) {
				lowerCasedRow[key.toLowerCase()] = value;
			}
			output.push(lowerCasedRow);
		});
		return output;
   }
   else { // outFormat === oracledb.OUT_FORMAT_ARRAY
		const output = [];
		result.forEach((row) => {
			const lowerCasedRow = {};
			for (let i = 0; i < row.length; i++) {
				lowerCasedRow[columnTypes[i].name] = row[i];
			}
			output.push(lowerCasedRow);
		});
		return output;
   }
};

// const nativeTypeToEvidenceType = function (dataTypeId, defaultType = undefined) {
/** @type {import("@evidence-dev/db-commons").RunQuery<oracledbOptions>} */
const runQuery = async (queryString, database = {}, batchSize = 100000, closeBeforeResults = false) => {
	try {
		//const outFormat = oracledb.OUT_FORMAT_OBJECT; // less performance fix later
		const outFormat = oracledb.OUT_FORMAT_ARRAY; // performance/memory
		if (database.thickMode) {
			oracledb.initOracleClient({libDir: database.libDir});
		}
		const credentials = {
			user: database.user,
			password: database.password,
			connectString: database.connectString,
			externalAuth: database.externalAuth,
			configDir: database.configDir
			};

    	await oracledb.createPool(credentials);

		const connection = await oracledb.getConnection();

		const cleanedString = cleanQuery(queryString);
	    /* Really need COUNT(*)? */
		const lengthQuery = await connection.execute(
			`WITH root as (${cleanedString}) SELECT COUNT(*) as rrows FROM root`,
			[], // no bind variables
			{
	        resultSet: false, // return a ResultSet (default is false)
	        outFormat: outFormat,
	        maxRows: 1
	      }
	    );
	    //console.dir(lengthQuery, {depth:null});
		const rowCount = (outFormat === oracledb.OUT_FORMAT_ARRAY) ? lengthQuery.rows[0][0] : lengthQuery.rows[0].RROWS;
		//console.log(`rowCount ${rowCount}`);

		result = await connection.execute(
			cleanedString,
			[], // no bind variables
			{
	        resultSet: true, // return a ResultSet (default is false)
	        outFormat: outFormat
	      }
	    );
		const rs = result.resultSet;
		try {
			const firstBatch = await rs.getRows(batchSize);
			const columnTypes = mapResultsToEvidenceColumnTypes(rs.metaData);
		    //console.dir(firstBatch, {depth:null});
		    //console.dir(rs.metaData, {depth:null});
		    //console.log(`expectedRowCount ${rowCount}`);
		    //console.log(`batchSize ${batchSize}`);

			return {
				rows: async function* () {
					try {
						yield standardizeResult(firstBatch, columnTypes, outFormat);
						if (firstBatch.length === batchSize) {
							let results;
							while ((results = await rs.getRows(batchSize)) && results.length > 0)
							    //console.dir(results, {depth:null});
								yield standardizeResult(results, columnTypes, outFormat);
					    }
						return;
					} finally {
						await rs.close();
						await connection.close();
					}
				},
				columnTypes: columnTypes,
				expectedRowCount: rowCount
			};
		} catch (e) {
			await rs.close();
			console.error(e);
			//await connection.release();
			//await pool.end();
			if (connection) {
			  try {
				await connection.close();
			  } catch (e) {
				console.error(e);
			  }
			}
			throw e;
		} finally {
			if (closeBeforeResults) {
				await rs.close();
				if (connection) {
				  try {
					await connection.close();
				  } catch (e) {
					console.error(e);
				  }
				}
			}
		}
	} catch (err) {
		if (err.message) {
			throw err.message.replace(/\n|\r/g, ' ');
		} else {
			throw err.replace(/\n|\r/g, ' ');
		}
	}
};

module.exports = runQuery;

/**
 * @typedef {Object} oracledbOptions
 * @property {string} connectString
 * @property {string} password
 */

/** @type {import('@evidence-dev/db-commons').GetRunner<oracledbOptions>} */
module.exports.getRunner = async (opts) => {
	return async (queryContent, queryPath, batchSize) => {
		// Filter out non-sql files
		if (!queryPath.endsWith('.sql')) return null;
		return runQuery(queryContent, opts, batchSize);
	};
};

/** @type {import('@evidence-dev/db-commons').ConnectionTester<oracledbOptions>} */
module.exports.testConnection = async (opts) => {
	return await runQuery('SELECT 1 FROM DUAL', opts) // true closeBeforeResults does not work
		.then(exhaustStream)
		.then(() => true)
		.catch((e) => ({ reason: e.message ?? (e.toString() || 'Invalid Credentials') }));
};

module.exports.options = {
    connectString: {
		title: 'Host:Port/ServiceName',
		secret: false,
		type: 'string',
		required: true
	},
	user: {
		title: 'Username',
		secret: false,
		type: 'string',
		required: true
	},
	password: {
		title: 'Password',
		secret: true,
		type: 'string',
		required: true
	},
	externalAuth: {
		title: 'externalAuth',
		type: 'boolean',
		default: false,
		secret: false,
		description: 'External Authentication'
	},
	thickMode: {
		title: 'thickMode',
		type: 'boolean',
		default: false,
		secret: false,
		description: 'Thick Mode'
	},
	configDir: {
		title: 'Oracle Config Dir',
		secret: false,
		type: 'string',
		required: false,
		description: 'Should be set when using TNSNAMES.ORA'
    },
	libDir: {
		title: 'Oralce Lib Dir',
		secret: false,
		type: 'string',
		required: false,
		description: 'OCI dir for Thick'
   }
};
