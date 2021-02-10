const mysql = require('mysql');
const fs = require('fs');

/************************************************************************************ 
 * Connection strings
************************************************************************************/
const config_flat = {
    host: 'localhost',
    database: 'dac_flat',
    port: 3389,
    user: 'dac_flat',
    password: '3c3ncprdjVMJeLdFu7Nr'
}
// add atomic configuration
const config_atomic = {
    host: 'localhost',
    database: 'dac',
    port: 3388,
    user: 'dac',
    password: 'rC9sdEmPC7pmMKanceUt'
}
/************************************************************************************ 
 * Command line interpreter
************************************************************************************/

const yargs = require('yargs')
  .usage("Usage : $0 -f xxx.txt -c")
  .help('help')
  .alias('help', 'h')
  .options({
    verbose: {
      alias: 'v',
      type: 'boolean',
      description : 'active verbose mode',
    },
    reportFilename: {
      alias: 'f',
      type: 'string',
      description : 'report filename',
      default : 'report.tsv'
    },
	stock_ecom : {
		alias: 'e',
        type: 'boolean',
        description : 'Generate for stock_ecom',
        default : false
	 },
	stock_retail : {
		alias: 'r',
        type: 'boolean',
        description : 'Generate for stock_retail',
        default : false
	 },
    insert: {
        alias: 'i',
        type: 'boolean',
        description : 'Generate insert on target database',
        default : false
      },
    update: {
        alias: 'u',
        type: 'boolean',
        description : 'Generate update on target database',
        default : false
      },
    update_target: {
        alias: 'o',
        type: 'boolean',
        description : 'Generate update on target when only present in target database',
        default : false
      },
    commit: {
      alias: 'c',
      type: 'boolean',
      description : 'Commmit changes on target database',
      default : false
    },
    limit: {
      alias: 'l',
      type: 'integer',
      description : 'set the maximum number of rows to read. Used for test only',
      default : 0
    },
  })
  .argv;

var reportFilename = yargs.reportFilename;
var logFile = fs.createWriteStream(reportFilename, {flags: 'w'});
var limit = yargs.limit;
var isVerbose = yargs.verbose;

// Action to do on target database
var isEcom = yargs.stock_ecom;
var isRetail = yargs.stock_retail;
var isCommit = yargs.commit;
var isCommit = yargs.commit;
var isInsert = yargs.insert;
var isUpdate = yargs.update;
var isUpdateTarget = yargs.update_target;

/************************************************************************************ 
 * Variables to manage both streams
************************************************************************************/
var streamFlat;
var streamAtomic;
var lastDataAtomic;
var lastDataFlat;
var doFlipFlap = false;
var goToEnd = false;

/************************************************************************************ 
 * Reporting
************************************************************************************/
const compareResultReason = {
	EQUALS: "equals",
	NOTEQUALS: "notEquals",
    SOURCEONLY: "sourceOnly",
    TARGETONLY: "targetOnly"
};

var statistics = {
    equals : 0,
    notEquals : 0,
    sourceOnly : 0,
    targetOnly : 0,
    total: 0,
    start: new Date(),
    end: null
};

function reportCompare(source, target, reason) {
    statistics.total++;
    var msg;
    switch (reason) {
        case compareResultReason.EQUALS:
            msg = `${source.sku}  ${source.country}  ${reason}`;
            statistics.equals++;
            break;
        case compareResultReason.NOTEQUALS:
            msg = `${source.sku}  ${source.country} ${source.last_update} ${lastDataFlat.last_update} ${reason}`;
            statistics.notEquals++;
            break;
        case compareResultReason.SOURCEONLY:
            msg = `${source.sku}  ${source.country}  ${reason}`;
            statistics.sourceOnly++;
            break;
        case compareResultReason.TARGETONLY:
            msg = `${target.sku}  ${target.country}  ${reason}`;
            statistics.targetOnly++;
            break;
    }
    logVervose(msg);
    logFile.write(msg + '\n');
    displayStatistics();
}

function formatPercentage(value) {
    return value.toLocaleString(undefined,{style: 'percent', minimumFractionDigits:2});
}

function displayStatistics(steps = 1000) {
    if (steps == 0 || statistics.total % steps == 0) {
        console.log(statistics);
    }
}

function endReport() {
    // Last stats
    displayStatistics(0);
    statistics.end = new Date();
    // Global report
    console.log( {
        equals : formatPercentage(statistics.equals / statistics.total),
        notEquals : formatPercentage(statistics.notEquals / statistics.total),
        sourceOnly : formatPercentage(statistics.sourceOnly / statistics.total),
        targetOnly : formatPercentage(statistics.targetOnly / statistics.total),
        duration : ((statistics.end - statistics.start) / 1000)
    });
}

function logVervose(data) {
    if (isVerbose) {
        console.log(data);
    }
}

/************************************************************************************ 
 * Main program
************************************************************************************/

// Creation both connections
const connectionAtomic = mysql.createPool(config_atomic, {
    multipleStatements: true
  });
const connectionFlat = mysql.createPool(config_flat, {
    multipleStatements: true
  });

const connectionWrite = mysql.createPool(config_flat, {
    multipleStatements: true
});

var closeCounter = 0;

function CloseStream(stream) {
    closeCounter++;
    if (stream && stream.isPaused()) {
        goToEnd = true;
        stream.resume();
    }
    if (closeCounter == 2) {
        logVervose("Close file");
        logFile.close();
        endReport();
        console.log("Process completed");
        process.exit();
    }
}

connectionAtomic.getConnection((err) => {
    if (err) throw err;

    connectionFlat.getConnection((err) => {
        if (err) throw err;
		if (isEcom) {
        	startQueryEcomAtomic();
		} else if (isRetail) {
			startQueryRetailAtomic();
		}
    });
});

async function startQueryEcomAtomic() {
    var query = `SELECT 
            CONCAT(sku, "#", country_code) as theKey
            , sku
            , country_code as country
            , JSON_EXTRACT(data, "$.has_stock") = "1" as stock_ecom
            , (JSON_EXTRACT(data, "$.is_sellable") = "1"
                AND JSON_EXTRACT(data, "$.is_visible") = "1"
                AND JSON_EXTRACT(data, "$.is_searchable") = "1") as publication_status
            , JSON_EXTRACT(metadata, "$.updated_at") as last_update
        FROM
            product_availability
        ORDER BY 
            sku, country`;
    if (limit > 0)
    query += ' LIMIT ?';
    var rowNumber = 0;
    streamAtomic = connectionAtomic.query(query, limit)
    .stream({highWaterMark: 50})
    .on('data', function (data) {
        rowNumber++;
        lastDataAtomic = data;
        logVervose(`A${rowNumber} => ${data.sku}`);
        // Bootstapper pour lancer le 2e flux en //
        if (rowNumber == 1) {
            streamAtomic.pause();
            startQueryEcomFlat();
            return;
        };
        if (doFlipFlap) {
            streamAtomic.pause();
            doFlipFlap = false;
            streamFlat.resume();
            return;
        }
        if (lastDataAtomic.theKey < lastDataFlat.theKey || goToEnd) {
            reportCompare(lastDataAtomic, null, compareResultReason.SOURCEONLY);
            insertEcomTarget(lastDataAtomic);
        } else if (lastDataAtomic.theKey == lastDataFlat.theKey) {
            let dateFlat = lastDataFlat.last_update? lastDataFlat.last_update.getTime() : null;
            let dateAtomic =  getDateFromString(lastDataAtomic.last_update);
            if (lastDataAtomic.stock_ecom == lastDataFlat.stock_ecom
                && lastDataAtomic.publication_status == lastDataFlat.publication_status
                && dateAtomic.getTime() == dateFlat) {
                    reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.EQUALS);
            }
            else {
                reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.NOTEQUALS);
                updateEcomTarget(lastDataAtomic);
            }
            // On donne la main au stream flat en indiquant qu'on veut avancer d'une position
            // Le flux flat redonnera la main au flux atomic en le faisant avancer d'une position à son tour
            doFlipFlap = true;
            streamAtomic.pause();
            streamFlat.resume();
            return;
        } else if (lastDataAtomic.theKey > lastDataFlat.theKey) {
            reportCompare(null, lastDataFlat, compareResultReason.TARGETONLY);
            onlyEcomTarget(lastDataFlat);
            streamAtomic.pause();
            streamFlat.resume();
            return;
        }
    })
    .on('end',function() { 
        logVervose('End of atomic stream reached');
        CloseStream(streamFlat);
    });
}

function startQueryRetailAtomic() {
    var query = `SELECT 
            CONCAT(sku, "#", country_code) as theKey
            , sku
            , country_code as country
            , CAST(presentability AS UNSIGNED) as stock_retail
            , hold_date as last_update
        FROM
            stock_retail
        ORDER BY 
            sku, country`;
    if (limit > 0)
    query += ' LIMIT ?';
    var rowNumber = 0;
    streamAtomic = connectionAtomic.query(query, limit)
    .stream({highWaterMark: 50})
    .on('data', function (data) {
        rowNumber++;
        lastDataAtomic = data;
        logVervose(`A${rowNumber} => ${data.sku}`);
        // Bootstapper pour lancer le 2e flux en //
        if (rowNumber == 1) {
            streamAtomic.pause();
            startQueryRetailFlat();
            return;
        };
 
        if (doFlipFlap) {
            streamAtomic.pause();
            doFlipFlap = false;
            streamFlat.resume();
            return;
        }
        
        if (lastDataAtomic.theKey < lastDataFlat.theKey || goToEnd) {
            reportCompare(lastDataAtomic, null, compareResultReason.SOURCEONLY);
            insertRetailTarget(lastDataAtomic);
        } else if (lastDataAtomic.theKey == lastDataFlat.theKey) {
            let dateFlat = lastDataFlat.last_update? lastDataFlat.last_update.getTime() : null;
            let dateAtomic = lastDataAtomic.last_update? lastDataAtomic.last_update.getTime() : null;
            if (lastDataAtomic.stock_retail == lastDataFlat.stock_retail
                && dateAtomic == dateFlat) {
                    reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.EQUALS);
            }
            else {
                reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.NOTEQUALS);
                updateRetailTarget(lastDataAtomic);
            }
            // On donne la main au stream flat en indiquant qu'on veut avancer d'une position
            // Le flux flat redonnera la main au flux atomic en le faisant avancer d'une position à son tour
            doFlipFlap = true;
            streamAtomic.pause();
            streamFlat.resume();
            return;
        } else if (lastDataAtomic.theKey > lastDataFlat.theKey) {
            reportCompare(null, lastDataFlat, compareResultReason.TARGETONLY);
            onlyRetailTarget(lastDataFlat);
            streamAtomic.pause();
            streamFlat.resume();
            return;
        }
    })
    .on('end',function() { 
        logVervose('End of atomic stream reached');
        CloseStream(streamFlat);
    });
}

async function startQueryEcomFlat() {
    var query = `SELECT
            CONCAT(sku, "#", country) as theKey
            , sku
            , country
            , ecom_stock_availability as stock_ecom
            , publication_status as publication_status
            , last_update_ecom_stock as last_update
        FROM
            sku_country   
        ORDER BY 
            sku, country`;
    if (limit > 0)
        query += ' LIMIT ?';

    var rowNumber = 0;
    streamFlat = connectionFlat.query(query, limit)
    .stream({highWaterMark: 50})
    .on('data', function (data) {
        rowNumber++;
        lastDataFlat = data;
        logVervose(`F${rowNumber} => ${data.sku}`);
        // stopFlat est à true quand le reader atomic souhaite faire avancer le reader flat d'une position
        if (doFlipFlap) {
            streamFlat.pause();
            doFlipFlap = false;
            streamAtomic.resume();
            return;
        }
        if (lastDataAtomic.theKey > lastDataFlat.theKey || goToEnd) {
            reportCompare(null, lastDataFlat, compareResultReason.TARGETONLY);
            onlyEcomTarget(lastDataFlat);
        } else if (lastDataAtomic.theKey == lastDataFlat.theKey) {
            let dateFlat = lastDataFlat.last_update? lastDataFlat.last_update.getTime() : null;
            let dateAtomic =  getDateFromString(lastDataAtomic.last_update);
            if (lastDataAtomic.stock_ecom == lastDataFlat.stock_ecom
                && lastDataAtomic.publication_status == lastDataFlat.publication_status
                && dateAtomic.getTime() == dateFlat) {
                    reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.EQUALS);
            } else {
                reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.NOTEQUALS);
                updateEcomTarget(lastDataAtomic);
               
            }
            doFlipFlap = true;
            streamAtomic.pause();
            streamFlat.resume();
            return;
        } else if (lastDataAtomic.theKey < lastDataFlat.theKey) {
            reportCompare(lastDataAtomic, null, compareResultReason.SOURCEONLY);
            insertEcomTarget(lastDataAtomic);
            streamFlat.pause();
            streamAtomic.resume();
            return;
        } 
    })
    .on('end',function() { 
        logVervose('End of flat stream reached');
        connectionFlat.end(); 
        CloseStream(streamAtomic);
    });
}


function startQueryRetailFlat() {
    var query = `SELECT
            CONCAT(sku, "#", country) as theKey
            , sku
            , country
            , retail_stock_availability as stock_retail
            , last_update_retail_stock as last_update
        FROM
            sku_country 
        ORDER BY 
            sku, country`;
    if (limit > 0)
        query += ' LIMIT ?';

    var rowNumber = 0;
    streamFlat = connectionFlat.query(query, limit)
    .stream({highWaterMark: 50})
    .on('data', function (data) {
        rowNumber++;
        lastDataFlat = data;
        logVervose(`F${rowNumber} => ${data.sku}`);
        // stopFlat est à true quand le reader atomic souhaite faire avancer le reader flat d'une position
        if (doFlipFlap) {
            streamFlat.pause();
            doFlipFlap = false;
            streamAtomic.resume();
            return;
        }
        if (lastDataAtomic.theKey > lastDataFlat.theKey || goToEnd) {
            reportCompare(null, lastDataFlat, compareResultReason.TARGETONLY);
            onlyRetailTarget(lastDataFlat);
        } else if (lastDataAtomic.theKey == lastDataFlat.theKey) {
            let dateFlat = lastDataFlat.last_update? lastDataFlat.last_update.getTime() : null;
            let dateAtomic = lastDataAtomic.last_update? lastDataAtomic.last_update.getTime() : null;
            if (lastDataAtomic.stock_retail == lastDataFlat.stock_retail
                && dateAtomic == dateFlat) {
                    reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.EQUALS);
            } else {
                reportCompare(lastDataAtomic, lastDataFlat, compareResultReason.NOTEQUALS);
                updateRetailTarget(lastDataAtomic);
            }
            doFlipFlap = true;
            streamAtomic.pause();
            streamFlat.resume();
            return;
        } else if (lastDataAtomic.theKey < lastDataFlat.theKey) {
            reportCompare(lastDataAtomic, null, compareResultReason.SOURCEONLY);
            insertRetailTarget(lastDataAtomic);
            streamFlat.pause();
            streamAtomic.resume();
            return;
        } 
    })
    .on('end',function() { 
        logVervose('End of flat stream reached');
        connectionFlat.end(); 
        CloseStream(streamAtomic);
    });
}

/************************************************************************************ 
 * Mediation functions
************************************************************************************/

function insertEcomTarget(data) {
    if (isCommit && isInsert) {
        let query = `INSERT INTO sku_country (sku, country, ecom_stock_availability, publication_status, last_update_ecom_stock) 
            VALUES (?, ?, ? , ?, ?)`;
        connectionFlat.query(query, [data.sku, data.country, data.stock_ecom, data.publication_status, getDateFromString(data.last_update)]);
    }
}

async function updateEcomTarget(data) {
    if (isCommit && isUpdate) {
            connectionWrite.getConnection();
            let query = `UPDATE sku_country SET ecom_stock_availability = ?,
            publication_status = ?,
            last_update_ecom_stock = ?
            WHERE sku = ? AND country = ?`;
            //console.log(connectionWrite.query(query, [data.stock_ecom, data.publication_status, getDateFromString(data.last_update), data.sku, data.country]));
            connectionWrite.query(query, [data.stock_ecom, data.publication_status, getDateFromString(data.last_update), data.sku, data.country], function (err, result, fields) {
                if (err) {
                    console.log("ERROR : ", err);
                }
                if (result) {
                    console.log("RESULT : ", result);
                }
            });
    }
}

function onlyEcomTarget(data) {
    if (isCommit && isUpdateTarget) {
        let query = `UPDATE sku_country SET ecom_stock_availability = 0,
        publication_status = 0,
        last_update_ecom_stock = null
        WHERE sku = ? AND country = ?`;
        connectionFlat.query(query, [data.sku, data.country]);
    }
}

function insertRetailTarget(data) {
    if (isCommit && isInsert) {
        let query = `INSERT INTO sku_country (sku, country, retail_stock_availability, last_update_retail_stock) 
            VALUES (?, ?, ? , ?)`;
        connectionFlat.query(query, [data.sku, data.country, data.stock_retail, getDateFromString(data.last_update)]);
    }
}

function updateRetailTarget(data) {
    if (isCommit && isUpdate) {
        let query = `UPDATE sku_country SET retail_stock_availability = ?,
        last_update_retail_stock = ?
        WHERE sku = ? AND country = ?`;
        connectionFlat.query(query, [data.stock_retail, getDateFromString(data.last_update), data.sku, data.country]);
    }
}

function onlyRetailTarget(data) {
    if (isCommit && isUpdateTarget) {
        let query = `UPDATE sku_country SET retail_stock_availability = 0,
        last_update_retail_stock = null
        WHERE sku = ? AND country = ?`;
        connectionFlat.query(query, [data.sku, data.country]);
    }
}

function getDateFromString(dateString) {
    dateString = dateString.replace('"', '').replace(/T/, ' ').replace(/\..+/, '');
    dateString = new Date(dateString);
    return dateString;
}