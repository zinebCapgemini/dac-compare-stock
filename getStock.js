/**
 * Sets the configuration and initializes the DB isConnected.
 */
import {createConnection} from "mysql";
import fs from 'fs'
import dateformat from 'dateformat';

// add flat configuration
const config_flat = {
    host: 'localhost',
    database: '',
    port: 3389,
    user: '',
    password: ''
}
// add atomic configuration
const config_atomic = {
    host: 'localhost',
    database: '',
    port: 3388,
    user: '',
    password: ''
}

try {
    Promise.all([getDataAtomic(), getDataFlat()]).then((values) => {
        console.log('Comparaison product_availability & sku_country');
        let resultAtomic = values[0]
        let resultFlat = values[1];
        let resultDiff = [];

        resultFlat.forEach(rowSkucountry => {
            let elemExist  = resultAtomic.find(atomic => atomic.sku === rowSkucountry.sku && atomic.country === rowSkucountry.country);

            if (elemExist && ( elemExist.stock_ecom !== rowSkucountry.stock_ecom || elemExist.publication_status !== rowSkucountry.publication_status)){
                resultDiff.push({
                    'sku' : rowSkucountry.sku,
                    'country' : rowSkucountry.country,
                    'stock_flat' : rowSkucountry.stock_ecom,
                    'stock_atomic' : elemExist.stock_ecom,
                    'last_update_flat' : rowSkucountry.last_update,
                    'last_update_atomic' : elemExist.last_update,
                    'publication_status_atomic' : elemExist.publication_status,
                    'publication_status_flat' : rowSkucountry.publication_status
                });
            }
        });
        let resultDiffA = [];
        addCSVDiff(['sku', 'country', 'stock_flat', 'stock_atomic', 'last_update_flat', 'last_update_atomic', 'publication_status_atomic', 'publication_status_flat'], resultDiff, ';', 'diff.csv');
        resultAtomic.forEach(atomic => {
            let elemExist  = resultFlat.find(flat => flat.sku === atomic.sku && flat.country === atomic.country);
            if (elemExist === undefined){
                resultDiffA.push(atomic);
            }
        });
        addCSV(['sku', 'country', 'stock', 'last_update'], resultDiffA, ';', 'diff_not_exist_flat.csv');
    });

} catch
    (error) {
    console.log('Error failed ' + error.message);
}


async function  getDataAtomic(){
    return new Promise( (resolve, reject) => {
        let atomicConnection = createConnection({
            host: config_atomic.host,
            port: config_atomic.port,
            user: config_atomic.user,
            password: config_atomic.password,
            database: config_atomic.database
        });
        console.log('Connexion à la base atomic en cours');
        let resultAtomic = [];

        atomicConnection.connect(function (err) {
            if (err) throw err;
            console.log('Connexion à la base Atomic OK');
            console.log('Query atomic cours');
            atomicConnection.query('' +
                'SELECT sku, \n' +
                '\n' +
                'JSON_EXTRACT(data, "$.has_stock") = "1" as stock_ecom , \n' +
                ' ( JSON_EXTRACT(data, "$.is_sellable") = "1"\n' +
                'AND JSON_EXTRACT(data, "$.is_visible") = "1"\n' +
                'AND JSON_EXTRACT(data, "$.is_searchable") = "1"\n' +
                '\n' +
                ') as publication_status\n' +
                ', country_code as country ,last_update \n' +
                ' FROM  ' +
                ' product_availability where country_code = "FR" ' +
                ' order by sku;'
                , function (err, result, fields) {
                    if (err) throw err;
                    resultAtomic = result;
                    console.log('Get ' + result.length +  ' elements from atomic database');
                    addCSV(['sku', 'country', 'stock', 'last_update', 'publication_status'], resultAtomic, ';', 'atomic.csv');
                    resolve(resultAtomic);
                });
        });
    });

}

async function  getDataFlat(){
    return new Promise( (resolve, reject) => {
        let flatConnection = createConnection({
            host: config_flat.host,
            port: config_flat.port,
            user: config_flat.user,
            password: config_flat.password,
            database: config_flat.database
        });
        console.log('Connexion à la base flat en cours');
        let resultFlat = [];

        flatConnection.connect(function (err) {
            console.log('Connexion à la base flat OK');
            console.log('Query flat en cours');
            if (err) throw err;
            flatConnection.query('select sku , ecom_stock_availability as stock_ecom ' +
                ' , country , last_update_ecom_stock as last_update \n , publication_status ' +
                ' FROM \n' +
                'sku_country \n' +
                'where country  = \'FR\'\n' +
                ' AND last_update_ecom_stock is not NULL ' +
                'order by sku;', function (err, result, fields) {
                if (err) throw err;
                resultFlat = result;
                console.log('Get ' + result.length +  ' elements from flat database');
                addCSV(['sku', 'country', 'stock', 'last_update', 'publication_status'], result, ';', 'flat.csv');
                resolve(resultFlat);
            });
        });
    });
}


function addCSV(arrayHeader, arrayData, delimiter, fileName) {
    let csvHeader = arrayHeader.join(delimiter) + '\n';
    arrayData.forEach(row => {
        let date =  row.last_update ? dateformat(new Date(row.last_update), 'yyyy-mm-dd h:MM:ss') : '';
        csvHeader += row.sku + delimiter + row.stock_ecom + delimiter + row.country
            + delimiter + date +  delimiter  +  row.publication_status +  "\n";
    });
    console.log('File Writing ....');
    fs.writeFileSync(fileName, csvHeader);
    console.log('File Writing OK');
    return true;

}

function addCSVDiff(arrayHeader, arrayData, delimiter, fileName) {
    let csvHeader = arrayHeader.join(delimiter) + '\n';
    arrayData.forEach(row => {
        let dateAtomic =  row.last_update_atomic ? dateformat(new Date(row.last_update_atomic), 'yyyy-mm-dd h:MM:ss') : '';
        let dateFlat =  row.last_update_flat ? dateformat(new Date(row.last_update_flat), 'yyyy-mm-dd h:MM:ss') : '';

        csvHeader += row.sku + delimiter + row.country  + delimiter + row.stock_flat + delimiter + row.stock_atomic + delimiter + dateFlat
            + delimiter + dateAtomic + delimiter + row.publication_status_atomic + delimiter + row.publication_status_flat + "\n";
    });
    console.log('File Writing comparaison file ....');
    fs.writeFileSync(fileName, csvHeader);
    console.log('File Writing comparaison file OK');
    return true;

}
