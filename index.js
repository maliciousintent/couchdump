/*jshint node:true, laxcomma:true, eqnull:true, indent:2, undef:true, unused:true */

'use strict';

var CONFIG = require('config').CONFIG
  , BACKUPS = require('config').BACKUPS
  , async = require('async')
  , request = require('request')
  , AWS = require('aws-sdk')
  , coolog = require('coolog')
  , bytes = require('bytes')
  , crypto = require('crypto')
  ;


var PREFIX = process.env.PREFIX || 'manual';

coolog.addChannel({ name: 'root', level: 'debug', appenders: ['console'] });
AWS.config.update({ region: CONFIG.AWS_REGION, accessKeyId: CONFIG.AWS_ACCESS_KEY_ID, secretAccessKey: CONFIG.AWS_SECRET_ACCESS_KEY, sslEnabled: true });

var s3client = new AWS.S3()
  , logger = coolog.logger('app.js')
  , countOk = 0
  , backupSize = 0
  , countFail = 0
  , runTimestamp = new Date().toISOString()
  ;


async.eachSeries(BACKUPS, function (item, nextItem) {
  logger.log('\n');
  logger.log('Backup for', item.name);
  
  request({
    url: _makeURL(item.url)
  , qs: { include_docs: true }
  , json: false
  , timeout: 900000000
  , strictSSL: true
  }, function (err, _, body) {
    if (err) {
      logger.error('Cannot get data from database for item', item.name);
      logger.error('Error is', err);
      
      countFail++;
      nextItem(null);
      return;
    }
    
    var md5 = crypto.createHash('md5')
      .update(body, 'utf8')
      .digest('base64');
    
    logger.log('\t-> dumped data for', item.name, (!!item.comment) ? '(' + item.comment + ')' : '');
    logger.log('\t-> size is', bytes(body.length));
    logger.log('\t-> dump timestamp', new Date());
    logger.log('\t-> content MD5', new Buffer(md5, 'base64').toString('hex'));
    
    var fileName = _makeKey(item.name);
    
    s3client.putObject({
      Key: fileName
    , Body: body
    , Bucket: CONFIG.AWS_BUCKET
    , ContentMD5: md5
    , ServerSideEncryption: 'AES256'
    , ACL: 'private'
    , ContentType: 'application/json'
    }, function (err, s3resp) {
      if (err) {
        logger.error('Cannot store backup for item', item.name);
        logger.error('Error is', err);
        
        countFail++;
        nextItem(null);
        return;
      }
      
      logger.log('\t-> s3 put timestamp', new Date());
      logger.log('\t-> s3 filename', fileName);
      logger.log('\t-> s3 ETag', s3resp.ETag);
      
      backupSize = backupSize + body.length;
      countOk++;
      nextItem(null);
    });
  });
  
}, function (err) {
  if (err) {
    logger.error('Error', err);
    return;
  }
  
  logger.log('Backups completed:');
  logger.log('\t-> OK', countOk, 'items', '(total size: ' + bytes(backupSize) + ')');
  logger.log('\t-> Failed', countFail, 'items');
  process.exit((countFail === 0));
});


function _makeURL(url) {
  return url + '/_all_docs';
}

function _makeKey(name) {
  return PREFIX + '-' + runTimestamp + '/' + name + '.json';
}

