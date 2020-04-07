// The way to use this program is: sudo node ./utils/load_all_gkg.js. So it should be run from above directory utils.
// That's why the reference to errors folder has a single dot


const path = require("path")
const fs = require('fs');
const moment = require('moment-timezone');
const async = require('async');
const http = require('http');

const dbName = 'gdelt_graph';
//const arangoURL = 'http://127.0.0.1';
const arangoPort = 8529;
const arangoURLFor_HTTPRequestsOnly = '127.0.0.1';

// __dirname is the full path of the currently executing file 
const directoryPath = path.join(__dirname, '..', 'data');
const files = fs.readdirSync(directoryPath);

let errLogger = fs.createWriteStream('./errors/err_log_' + moment().format('YYYY-MM-DD') + '.txt', { flags: 'a' /* 'a' for appending  */});

//const auth = 'Basic ' + new Buffer.from('arango_flights_user:arango_U$ser_01').toString('base64');
const auth = 'Basic ' + new Buffer.from('gdelt_user:gdelt_u$3r').toString('base64');
// hostname cannot contain the 'http://'' it can only contain the plain hostname.
// take out 'x-arango-async':'store' to see response

const bndry = 'XXXsubpartXXX';
const postBatchOptions = {hostname: arangoURLFor_HTTPRequestsOnly, port:arangoPort, path:'/_db/' + dbName + '/_api/batch', method:'POST',
                headers:{'Authorization':auth,'Content-type':'multipart/form-data','boundary':bndry} };
//// This custom agent is more for multi request processing, conection reuse (pooling).  
//// It is less important for the purpose of this example, yet important because http handshake is expensive
//// Ultimately if you're not using pooling it's ok because the default agent is keeping the one connection alive
//const customAgent = new http.Agent({maxSockets: 3, keepAlive: true, keepAliveMsecs: 1000 });
//postBatchOptions.agent = customAgent;

let batchData = '';

(async function () {
    let counter = 0;
    let stock_counter = 0;
    let leftOver = '';
    let origPath;
    let replPath;
    let tempLinesArray;
    let localArr;
    let readStream;

    for (let i = 0; i < files.length; i++){
        if (files[i].indexOf('gkg.csv') > 0 && files[i].indexOf('gkg.csv.zip') < 0 ){ // we don't want the zip files
            counter = 0;
            stock_counter = 0;
            leftOver = '';
            origPath = path.join(directoryPath,files[i]);
            replPath = origPath.replace('gkg.csv','gkg_p.csv');
            
            
            readStream = fs.createReadStream(origPath, 'utf8');
            //fs.renameSync( origPath, replPath);
            console.log( 'just replaced: ' +  origPath + ' with: ' +  replPath);  
            for await (let chunk of readStream) {
                chunk = leftOver + chunk;
                tempLinesArray = chunk.split('\n');
                leftOver = tempLinesArray[tempLinesArray.length - 1];
                for(let idx = 0; idx < (tempLinesArray.length - 1); idx++ ){
                    counter++;
                    localArr = tempLinesArray[idx].split('\t');
                    let resp = await awaitable_promise_return_func(counter );

                    // we are only interested in the stock market and corona virus for now
                    
                    let pos1 = -1;
                    let pos2 = -1;
                    let pos3 = -1;
                    let pos4 = -1;

                    if (localArr[7] && localArr[7].length > 0 ){
                        pos1 = localArr[7].indexOf('ECON_STOCKMARKET');
                        pos3 = localArr[7].indexOf('TAX_DISEASE_CORONAVIRUS');
                    }
                    if (localArr[8] && localArr[8].length > 0 ){
                        pos2 = localArr[8].indexOf('ECON_STOCKMARKET');
                        pos4 = localArr[8].indexOf('TAX_DISEASE_CORONAVIRUS');
                    }

                    if ( pos1 > -1 || pos2 > -1 || pos3 > -1 || pos4 > -1){
                        stock_counter++;
                        //let v1_themes = localArr[7].substring(pos1, (pos1 + 16) );
                        //let v2_themes = localArr[8].substring(pos2, (pos2 + 16) );

                        let toneArr = localArr[15].split(',');

                        batchData = '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 'POST /_api/document/docs HTTP/1.1' 
                            + '\r\n\r\n' + JSON.stringify( {'_key':localArr[0],'dt':localArr[1],'src_type':localArr[2],'site':localArr[3],'link':localArr[4],
                                'v2_counts':localArr[6],'themes':'ECON_STOCKMARKET,TAX_DISEASE_CORONAVIRUS','v1_orgs':localArr[13],'tone':parseFloat(toneArr[0]),
                                 'pos_tone':parseFloat(toneArr[1]),'neg_tone':parseFloat(toneArr[2]),'polar_pctg':parseFloat(toneArr[3])} ) + '\r\n';

                        let locArray = [];
                        let cntryObj = {};
                        if (localArr[9].length > 0){
                            locArray = localArr[9].split(';');
                            for (let k = 0; k < locArray.length; k++){
                                let tmpSetArr = locArray[k].split('#');
                                let cntryKey = tmpSetArr[2] + '_' + localArr[1];
                                if ( !cntryObj[ cntryKey ] ){
                                    cntryObj[ cntryKey ] = '';
                                    batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/countries HTTP/1.1' + '\r\n\r\n' + JSON.stringify({'_key':cntryKey,'dt':localArr[1]}) + '\r\n';
                                    let locKey = cntryKey + '-' + localArr[0];
                                    let fromKey = 'countries/' + cntryKey;
                                    let toKey = 'docs/' + localArr[0];
                                    batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/countries_docs HTTP/1.1' + '\r\n\r\n' + 
                                            JSON.stringify({'_key':locKey ,'_from':fromKey,'_to':toKey,'tone':localArr[15]}) + '\r\n';
                                }
                            }
                        }

                        let prsnsArray = [];
                        if (localArr[11].length > 0){
                            prsnsArray = localArr[11].split(';');
                            for (let k = 0; k < prsnsArray.length; k++){
                                prsnsArray[k] = prsnsArray[k].replace(' ', '_');
                                batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/people HTTP/1.1' + '\r\n\r\n' + JSON.stringify({'_key':prsnsArray[k]}) + '\r\n';
                                let locKey = prsnsArray[k] + '-' + localArr[0];
                                let fromKey = 'people/' + prsnsArray[k];
                                let toKey = 'docs/' + localArr[0];
                                batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/people_docs HTTP/1.1' + '\r\n\r\n' + 
                                            JSON.stringify({'_key':locKey ,'_from':fromKey,'_to':toKey,'tone':localArr[15]}) + '\r\n';
                            }
                        }
                        batchData += '--'+bndry+'--';
                        
                        try {
                            let awaitResult = await postBatchWithAwaitableSetTimeout( postBatchOptions,batchData );
                            console.log('awaitResult: ' + JSON.stringify(awaitResult) );
                        }
                        catch(err){
                            console.log('promiseResp error (on data): ' + err);
                            errLogger.write( moment().format() + ' ' + err) // append string to your file
                        }
                        
                        //console.log();
                        //console.log('countries: ' + JSON.stringify(cntryObj) );
                        //console.log('persons: ' + prsnsArray );
                        //console.log(`${counter}/${stock_counter}  REC: ${localArr[0]} / V2.1DATE: ${localArr[1]} / TONE: ${localArr[15]}`);

                        //console.log(`${counter}/${stock_counter}  REC: ${localArr[0]} / V2.1DATE: ${localArr[1]} / V2_SRC_TYPE ${localArr[2]} / V2_SITE:` +
                        //` ${localArr[3]}  / V2_DOC: ${localArr[4]} / V1_COUNTS: ${localArr[5]} / V2.1_COUNTS: ${localArr[6]} / V1_THEMES: ECON_STOCKMARKET /` + 
                        //` V2_THEMES: ECON_STOCKMARKET / V1_LOCS: ${localArr[9]} / V1_PRSNS: ${localArr[11]} / V1_ORGS: ${localArr[13]} / TONE: ${localArr[15]}`);
                    }
                    
                }
            }

            if (leftOver.length > 0){ // the last row can be an empty string
                counter++;
                localArr = leftOver.split('\t');
                let resp = await awaitable_promise_return_func(counter + ' simulates response of insert: ' + origPath + ' / ' + localArr[0] + ' / ' + localArr[1]);
                
                // we are only interested in the stock market for now
                let pos1 = localArr[7].indexOf('ECON_STOCKMARKET');
                let pos2 = localArr[8].indexOf('ECON_STOCKMARKET');
                if ( pos1 > -1 || pos2 > -1 ){
                        stock_counter++;
                        //let v1_themes = localArr[7].substring(pos1, (pos1 + 16) );
                        //let v2_themes = localArr[8].substring(pos2, (pos2 + 16) );
                        let toneArr = localArr[15].split(',');

                        batchData = '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 'POST /_api/document/docs HTTP/1.1' 
                            + '\r\n\r\n' + JSON.stringify( {'_key':localArr[0],'dt':localArr[1],'src_type':localArr[2],'site':localArr[3],'link':localArr[4],
                                'v2_counts':localArr[6],'themes':'ECON_STOCKMARKET,TAX_DISEASE_CORONAVIRUS','v1_orgs':localArr[13],'tone':parseFloat(toneArr[0]),
                                 'pos_tone':parseFloat(toneArr[1]),'neg_tone':parseFloat(toneArr[2]),'polar_pctg':parseFloat(toneArr[3])} ) + '\r\n';

                        let locArray = [];
                        let cntryObj = {};
                        if (localArr[9].length > 0){
                            locArray = localArr[9].split(';');
                            for (let k = 0; k < locArray.length; k++){
                                let tmpSetArr = locArray[k].split('#');
                                let cntryKey = tmpSetArr[2] + '_' + localArr[1];
                                if ( !cntryObj[ cntryKey ] ){
                                    cntryObj[ cntryKey ] = '';
                                    batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/countries HTTP/1.1' + '\r\n\r\n' + JSON.stringify({'_key':cntryKey,'dt':localArr[1]}) + '\r\n';
                                    let locKey = cntryKey + '-' + localArr[0];
                                    let fromKey = 'countries/' + cntryKey;
                                    let toKey = 'docs/' + localArr[0];
                                    batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/countries_docs HTTP/1.1' + '\r\n\r\n' + 
                                            JSON.stringify({'_key':locKey ,'_from':fromKey,'_to':toKey,'tone':localArr[15]}) + '\r\n';
                                }
                            }
                        }

                        let prsnsArray = [];
                        if (localArr[11].length > 0){
                            prsnsArray = localArr[11].split(';');
                            for (let k = 0; k < prsnsArray.length; k++){
                                prsnsArray[k] = prsnsArray[k].replace(' ', '_');
                                batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/people HTTP/1.1' + '\r\n\r\n' + JSON.stringify({'_key':prsnsArray[k]}) + '\r\n';
                                let locKey = prsnsArray[k] + '-' + localArr[0];
                                let fromKey = 'people/' + prsnsArray[k];
                                let toKey = 'docs/' + localArr[0];
                                batchData += '--'+bndry + '\r\n' + 'Content-Type: application/x-arango-batchpart' + '\r\n\r\n' + 
                                            'POST /_api/document/people_docs HTTP/1.1' + '\r\n\r\n' + 
                                            JSON.stringify({'_key':locKey ,'_from':fromKey,'_to':toKey,'tone':localArr[15]}) + '\r\n';
                            }
                        }

                        batchData += '--'+bndry+'--';
                        
                        try {
                            let awaitResult = await postBatchWithAwaitableSetTimeout( postBatchOptions,batchData );
                            console.log('awaitResult: ' + JSON.stringify(awaitResult) );
                        }
                        catch(err){
                            console.log('promiseResp error (on data): ' + err);
                            errLogger.write( moment().format() + ' ' + err) // append string to your file
                        }

                        //console.log();
                        //console.log('countries: ' + JSON.stringify(cntryObj) );
                        //console.log(`${counter}/${stock_counter}  REC: ${localArr[0]} / V2.1DATE: ${localArr[1]} / TONE: ${localArr[15]}`);
                        
                        //console.log(`${counter}/${stock_counter}  REC: ${localArr[0]} / V2.1DATE: ${localArr[1]} / V2_SRC_TYPE ${localArr[2]} / V2_SITE:` +
                        //` ${localArr[3]}  / V2_DOC: ${localArr[4]} / V1_COUNTS: ${localArr[5]} / V2.1_COUNTS: ${localArr[6]} / V1_THEMES: ECON_STOCKMARKET /` + 
                        //` V2_THEMES: ECON_STOCKMARKET / V1_LOCS: ${localArr[9]} / V1_PRSNS: ${localArr[11]} / V1_ORGS: ${localArr[13]} / TONE: ${localArr[15]}`);
                }  

            }           

            //fs.renameSync( origPath, replPath);
            //console.log( 'just replaced: ' +  origPath + ' with: ' +  replPath);  

        }// end of if (files[i].indexOf('gkg.csv') > 0 ){

    }

})();




function postBatchWithAwaitableSetTimeout( argOptions, argPostBody ){
    return new Promise ((resolve, reject) => {
        //console.log('postDataForArango FROM LAST: ' + JSON.stringify(postDataForArango) );
        let returnData = ''
        let allData = '';
        let post_req = http.request(argOptions);
        post_req.end( argPostBody );
        post_req.on('error', function (err) {
            //console.log('Arango Post Error from postFunc: ' + err.toString() );
            reject(err);
        });
        post_req.on('response', res => {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                allData += chunk;
            });
            res.on('error', function (err) { // error will be in the response bellow under res.on('end' ,,,,
                reject(err);
            });
            res.on('end', () => {
                setTimeout( function() { resolve(  'Full resp: ' + allData  ); }, 0   );
                
            }); // end of res.on('end',  () => {
            
        }); // end of post_req.on('response', res => {

    }); // end of new Promise( ...)
}


function postFuncWithAwaitableSetTimeout( argOptions, argPostBody ){

    return new Promise ((resolve, reject) => {
        //console.log('postDataForArango FROM LAST: ' + JSON.stringify(postDataForArango) );
        let returnData = ''
        let allData = '';
        let post_req = http.request(argOptions);
        post_req.end( argPostBody );
        post_req.on('error', function (err) {
            //console.log('Arango Post Error from postFunc: ' + err.toString() );
            reject(err);
        });
        post_req.on('response', res => {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                allData += chunk;
            });
            res.on('error', function (err) { // error will be in the response bellow under res.on('end' ,,,,
                reject(err);
            });
            res.on('end', () => {
                setTimeout( function() { resolve(  'Full resp: ' + allData  ); }, 0   );
                
            }); // end of res.on('end',  () => {
            
        }); // end of post_req.on('response', res => {

    }); // end of new Promise( ...)
    
}

function awaitable_promise_return_func(argInput){
    return new Promise ( (resolve, reject) => {
        //setTimeout( function() { console.log('argInput: ' + argInput); }, 20    );
        //setTimeout(  resolve(argInput), 1000    );

        setTimeout( function() { resolve(argInput); }, 0 );
    });
}
