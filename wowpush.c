//
// http://wow.metoffice.gov.uk/automaticreading?siteid=123456&siteAuthenticationKey=654321
// &dateutc=2011-02-02+10%3A32%3A55&winddir=230&windspeedmph=12&windgustmph=12&windgustdir=25
// &humidity=90&dewptf=68.2&tempf=70&rainin=0&dailyrainin=5&baromin=29.1&soiltempf=25
// &soilmoisture=25&visibility=25&softwaretype=weathersoftware1.0
//

#define _XOPEN_SOURCE
#include <stdio.h>
#include <sqlite3.h>
#include <stdlib.h>
//#include <stddef.h>
#include <string.h>
#include <curl/curl.h>
#include <time.h>
#include <syslog.h>
#include "wowpush.h"


static int callback(void *NotUsed, int argc, char **argv, char **azColName){
	int i;
	for(i=0; i<argc; i++){
		printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	}
	printf("\n");
	return 0;
}


int postToWow(char *url) {
	CURL *curl;
	CURLcode res;
	curl_global_init(CURL_GLOBAL_DEFAULT);
	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);
		if(CURLE_OK != res) {
			syslog (LOG_INFO, "Failed Curl request");
			fprintf(stderr,"curl eror: %d\n", res);

			return 1;
		}
	}
	return 0;
}


int main () {
	sqlite3 * db;
	char * sql;
	sqlite3_stmt * stmt;
	int row = 0;
	int flag;
	char unsigned const * timestamp;


	openlog ("wowPush",LOG_PERROR, 0);
	syslog (LOG_INFO, "Commencing MetOffice WOW data push");


	CALL_SQLITE (open ("/home/pi/weather/data/datalogger.db", & db));
	sql = "SELECT replace(replace(rd3.timestamp,':','%3A'),' ','+'), rd3.wind_direction, rd3.wind_avg_kmh, \
		rd3.wind_gust_kmh, rd3.out_humidity,dd.dew_point, rd3.out_temp, rd3.rainfall, rd3.pressureAlt, \
		(rd1.mxRainD - rd1.mnRainD) AS rainD, (rd2.mxRainH - rd2.mnRainH) AS rainD, rd3.timestamp, rd3.flagPosted \
		FROM (SELECT MAX(timestamp) AS lastStamp, MIN(out_temp) AS mnTempD, MAX(out_temp) AS mxTempD, \
		MIN(rainfall) AS mnRainD, MAX(rainfall) AS mxRainD \
		FROM raw_data WHERE timestamp > DATETIME('now','start of day')) rd1 \
		JOIN (SELECT MAX(timestamp) AS lastStamp, MIN(out_temp) AS mnTempH, MAX(out_temp) AS mxTempH, \
		MIN(rainfall) AS mnRainH, MAX(rainfall) AS mxRainH \
		FROM raw_data WHERE timestamp > DATETIME('now','-1 hour')) rd2 ON rd1.lastStamp = rd2.lastStamp \
		JOIN raw_data rd3 ON rd1.lastStamp = rd3.timestamp \
		JOIN derived_data dd ON rd1.lastStamp = dd.timestamp;";
	CALL_SQLITE (prepare_v2 (db, sql, strlen (sql) + 1, & stmt, NULL));

 	while (1) {
        	int s;

        	s = sqlite3_step (stmt);
		if (s == SQLITE_ROW) {
			float temp, pres, wind, gust, dewp, raiD, raiH;
			int humi, dire;
			char wowReq[300];
			timestamp  = sqlite3_column_text (stmt, 0);
			temp  = sqlite3_column_double (stmt, 6) * 9 / 5 + 32;	// convert deg C to deg F
			humi  = sqlite3_column_int (stmt, 4);
			pres  = sqlite3_column_double (stmt, 8) / 33.86389;	// convert millibar to inches of mercury
			wind  = sqlite3_column_double (stmt, 2) * 0.621371;	// convert kph to mph
			gust  = sqlite3_column_double (stmt, 3) * 0.621371;	// convert kph to mph
			dire  = sqlite3_column_int (stmt, 1) * 22.5;		// convert 0-15 (N,NNE,NS..) to degrees 
//			rain  = sqlite3_column_double (stmt, 7) / 100;		// convert hundredths to inches
			raiD  = sqlite3_column_double (stmt, 9) / 100;		// convert hundredths to inches
			raiH  = sqlite3_column_double (stmt, 10) / 100;		// convert hundredths to inches
			dewp  = sqlite3_column_double (stmt, 5) * 9 / 5 + 32;	// convert deg C to deg F
			flag  = sqlite3_column_int (stmt, 12);

			sprintf (wowReq,"http:\x2f\x2fwow.metoffice.gov.uk/automaticreading?siteid=901466001&siteAuthenticationKey=111205&dateutc=%s&winddir=%d&windspeedmph=%.2f&windgustmph=%.2f&humidity=%d&dewptf=%.2f\
&tempf=%.2f&rainin=%.2f&dailyrainin=%.2f&baromin=%.4f&softwaretype=projivity1.0", 
			timestamp, dire, wind, gust, humi, dewp, temp, raiH, raiD, pres);

			timestamp  = sqlite3_column_text (stmt, 11);
			syslog (LOG_INFO, "Sqlite DB sampled");

			if (!flag) {
// Curl GET to WOW
				postToWow(wowReq);
				printf("New record posted for %s\n",timestamp);
				syslog (LOG_INFO, "New record posted");

			} else {
				printf("Last record already posted\n");
				syslog (LOG_INFO, "Last record already posted");

			}
			row++;
		} else if (s == SQLITE_DONE) {
			break;
		} else {
			fprintf (stderr, "Failed.\n");
			exit (1);
		}
	}
	if (!flag) {
		char *zErrMsg = 0;
		char wowUpdate[100];
		int s;
		sql = "UPDATE raw_data SET flagPosted = 1 WHERE timestamp=DATETIME(\"%s\");";
		sprintf(wowUpdate, sql, timestamp);
		s = sqlite3_exec(db, wowUpdate, callback, 0, &zErrMsg);
   		if( s != SQLITE_OK ) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
	}
	sqlite3_close(db);
	closelog ();
	return 0;
}
