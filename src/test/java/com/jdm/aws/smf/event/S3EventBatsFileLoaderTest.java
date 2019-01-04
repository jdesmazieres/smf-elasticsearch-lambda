package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class S3EventBatsFileLoaderTest {
	final String bats_file = "src/test/resources/bats-BXESymbols.csv";
	final S3EventBatsFileLoader loader = spy(S3EventBatsFileLoader.class);
	final Context context = mock(Context.class);
	final LambdaLogger log = mock(LambdaLogger.class);

	final ArgumentCaptor<String> bucketCptor = ArgumentCaptor.forClass(String.class);
	final ArgumentCaptor<String> keyCptor = ArgumentCaptor.forClass(String.class);
	final ArgumentCaptor<String> contentCptor = ArgumentCaptor.forClass(String.class);

	@Before
	public void setUp() throws Exception {
		doReturn(log).when(context)
				.getLogger();
		doAnswer(invocation -> {
			Object arg0 = invocation.getArgument(0);
			System.out.println(arg0);
			return null;
		}).when(log)
				.log(anyString());
		doNothing().when(loader)
				.storeS3FileContent(any(), any(), any());
		doReturn("archive").when(loader)
				.archiveS3File(any(), any());
	}

	@Test
	public void processS3File() throws Exception {
		final String csvBats = new String(Files.readAllBytes(Paths.get(bats_file)));
		loader.processS3File("scrBucket", "srcKey", csvBats, null, context);
		verify(loader, times(4)).processCsvRow(any(), any(), any(), any(), any());
		verify(loader, times(4)).storeS3FileContent(any(), any(), contentCptor.capture());
		verify(loader, times(1)).archiveS3File(any(), any());
	}

	@Test
	public void convertToJsonFiles() throws Exception {
		final String content = "environment=PROD,created=2018-06-16,time=21:10Z,warning=\n" +
				"company_name,bats_name,isin,currency,mic,reuters_exchange_code,lis_local,live,tick_type,reference_price,bats_prev_close,live_date,bloomberg_primary,bloomberg_bats,mifid_share,asset_class,matching_unit,euroccp_enabled,xclr_enabled,lchl_enabled,reuters_ric_primary,reuters_ric_bats,reference_adt_eur,csd,corporate_action_status,supported_services,trading_segment,printed_name,periodic_auction_max_duration,periodic_auction_min_order_entry_size,periodic_auction_min_order_entry_notional,max_otr_count,max_otr_volume,capped,venue_cap_percentage,venue_uncap_date\n" +
				"adidas AG,ADSd,DE000A1EWWW0,EUR,XETR,DE,650000.0000,t,mifid_5,198.90000000,198.90000000,2008-11-19,ADS GY Equity,ADS EB Equity,t,EQTY,7,t,t,t,,,234980962.91277629,DAKVDEFF,,EOPR,MTF,ADSd,100,0,0,100000,500000,0,2.103,";

		loader.convertToJsonFiles("destBucket", "destKey", content, context);
		verify(loader, times(1)).processCsvRow(any(), any(), any(), any(), any());
		verify(loader, times(1)).storeS3FileContent(any(), any(), contentCptor.capture());
		final String json = contentCptor.getValue();
		System.out.println(json);
		assertThat(json).startsWith("{");
		assertThat(json).contains("\"symbol\" : \"ADSd\"");
		assertThat(json).contains("\"class\" : \"EQTY\"");
		assertThat(json).contains("\"type\" : \"BXESymbols\"");
		assertThat(json).contains("\"provider\" : \"Bats\"");
		assertThat(json).contains("\"mic\" : \"XETR\"");
		assertThat(json).contains("\"isin\" : \"DE000A1EWWW0\"");
		assertThat(json).contains("\"currency\" : \"EUR\"");
		assertThat(json).endsWith("}");
	}

	@Test
	public void processCsvRow() {
		final String[] headers = "company_name,bats_name,isin,currency,mic,reuters_exchange_code,lis_local,live,tick_type,reference_price,bats_prev_close,live_date,bloomberg_primary,bloomberg_bats,mifid_share,asset_class,matching_unit,euroccp_enabled,xclr_enabled,lchl_enabled,reuters_ric_primary,reuters_ric_bats,reference_adt_eur,csd,corporate_action_status,supported_services,trading_segment,printed_name,periodic_auction_max_duration,periodic_auction_min_order_entry_size,periodic_auction_min_order_entry_notional,max_otr_count,max_otr_volume,capped,venue_cap_percentage,venue_uncap_date".split(",");
		final String[] line = "adidas AG,ADSd,DE000A1EWWW0,EUR,XETR,DE,650000.0000,t,mifid_5,198.90000000,198.90000000,2008-11-19,ADS GY Equity,ADS EB Equity,t,EQTY,7,t,t,t,,,234980962.91277629,DAKVDEFF,,EOPR,MTF,ADSd,100,0,0,100000,500000,0,2.103, ".split(",");

		assertThat(headers).describedAs("Les nombre de colonnes du header est incorrect")
				.hasSize(line.length);

		loader.processCsvRow(line, headers, "destBucket", "destKey", context);
		verify(loader, times(1)).storeS3FileContent(bucketCptor.capture(), keyCptor.capture(), contentCptor.capture());

		assertThat(bucketCptor.getValue()).isEqualToIgnoringCase("destBucket");
		assertThat(keyCptor.getValue()).isEqualToIgnoringCase("instrument/source/bats-ADSd.json");
		final String json = contentCptor.getValue();
		System.out.println(json);
		assertThat(json).startsWith("{");
		assertThat(json).contains("\"symbol\" : \"ADSd\"");
		assertThat(json).contains("\"class\" : \"EQTY\"");
		assertThat(json).contains("\"type\" : \"BXESymbols\"");
		assertThat(json).contains("\"provider\" : \"Bats\"");
		assertThat(json).contains("\"mic\" : \"XETR\"");
		assertThat(json).contains("\"isin\" : \"DE000A1EWWW0\"");
		assertThat(json).contains("\"currency\" : \"EUR\"");
		assertThat(json).endsWith("}");
	}
}
