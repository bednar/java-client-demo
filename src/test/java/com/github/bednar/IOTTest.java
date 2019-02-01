/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.github.bednar;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.Nonnull;

import org.influxdata.flux.domain.FluxTable;
import org.influxdata.platform.PlatformClient;
import org.influxdata.platform.PlatformClientFactory;
import org.influxdata.platform.QueryClient;
import org.influxdata.platform.WriteClient;
import org.influxdata.platform.domain.OnboardingResponse;
import org.influxdata.platform.error.rest.UnprocessableEntityException;
import org.influxdata.platform.write.Point;

import de.vandermeer.asciitable.AsciiTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jakub Bednar (bednar@github) (30/01/2019 12:07)
 */
public class IOTTest {

    private static final Logger LOG = LoggerFactory.getLogger(IOTTest.class);

    private static String url = "http://localhost:9999";

    private static String bucketID;
    private static String orgID;
    private static String token;

    @BeforeClass
    public static void onboarding() throws Exception {

        String username = "admin";
        String password = "111111";
        
        try {

            //
            // Do onboarding
            //
            OnboardingResponse response = PlatformClientFactory
                    .onBoarding(url, username, password, "Testing", "my-bucket");

            bucketID = response.getBucket().getId();
            orgID = response.getOrganization().getId();
            token = response.getAuthorization().getToken();

        } catch (UnprocessableEntityException exception) {

            //
            // Onboarding already done
            //
            PlatformClient platformClient = PlatformClientFactory.create(url, username, password.toCharArray());

            bucketID = platformClient.createBucketClient().findBuckets().get(0).getId();
            orgID = platformClient.createOrganizationClient().findOrganizations().get(0).getId();
            token = platformClient.createAuthorizationClient().findAuthorizations().get(0).getToken();

            platformClient.close();
        }
    }

    @Test
    public void iot() throws Exception {

        //
        // Init Client
        //
        PlatformClient platform = PlatformClientFactory.create(url, token.toCharArray());

        //
        // Write IOT Data
        //

        WriteClient writeClient = platform.createWriteClient();

        Instant now = Instant.ofEpochSecond(1548851316);

        // Mensuration 1
        Point weatherOutdoor1 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 980)
                .addField("wind_speed", 10)
                .addField("precipitation", 860)
                .addField("battery_voltage", 2.6)
                .time(now, ChronoUnit.SECONDS);

        writeClient.writePoint(bucketID, orgID, weatherOutdoor1);

        // Mensuration 2
        Point weatherOutdoor2 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 860)
                .addField("wind_speed", 12)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.plus(10, ChronoUnit.SECONDS), ChronoUnit.SECONDS);

        writeClient.writePoint(bucketID, orgID, weatherOutdoor2);

        // Mensuration 3
        Point weatherOutdoor3 = Point.measurement("weather_outdoor")
                .addTag("home", "100")
                .addTag("sensor", "120")
                .addField("pressure", 880)
                .addField("wind_speed", 11)
                .addField("precipitation", 865)
                .addField("battery_voltage", 2.6)
                .time(now.plus(20, ChronoUnit.SECONDS), ChronoUnit.SECONDS);

        writeClient.writePoint(bucketID, orgID, weatherOutdoor3);
        writeClient.close();

        //
        // Querying by Flux
        //
        QueryClient queryClient = platform.createQueryClient();

        // Last Mensuration
        String flux = "from(bucket: \"my-bucket\")\n"
                + "  |> range(start: 0)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")\n"
                + "  |> last()";

        List<FluxTable> tables = queryClient.query(flux, orgID);
        printResult(flux, tables);

        // Average pressure
        flux = "from(bucket: \"my-bucket\")\n"
                + "  |> range(start: 0)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r._field == \"pressure\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")\n"
                + "  |> mean()";

        tables = queryClient.query(flux, orgID);
        printResult(flux, tables);

        // Max Wind Speed
        flux = "from(bucket: \"my-bucket\")\n"
                + "  |> range(start: 0)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r._field == \"wind_speed\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")\n"
                + "  |> max()";

        tables = queryClient.query(flux, orgID);
        printResult(flux, tables);

        // Select All
        flux = "from(bucket: \"my-bucket\")\n"
                + "  |> range(start: 0)\n"
                + "  |> filter(fn: (r) => r._measurement == \"weather_outdoor\")\n"
                + "  |> filter(fn: (r) => r.home == \"100\")\n"
                + "  |> filter(fn: (r) => r.sensor == \"120\")";

        tables = queryClient.query(flux, orgID);
        printResult(flux, tables);

        platform.close();
    }

    private void printResult(final String flux, @Nonnull final List<FluxTable> tables) {

        AsciiTable at = new AsciiTable();

        at.addRule();
        at.addRow("table", "_start", "_stop", "_time", "_measurement", "_field", "_value");
        at.addRule();

        tables.forEach(table -> table.getRecords().forEach(record -> {

            String tableIndex = format(record.getTable());
            String start = format(record.getStart());
            String stop = format(record.getStop());
            String time = format(record.getTime());

            String measurement = format(record.getMeasurement());
            String field = format(record.getField());
            String value = format(record.getValue());

            at.addRow(tableIndex, start, stop, time, measurement, field, value);
            at.addRule();
        }));

        LOG.info("\n\nQuery:\n\n{}\n\nResult:\n\n{}\n", flux, at.render(150));
    }

    private String format(final Object start) {

        if (start == null) {
            return "";
        }

        if (start instanceof Instant) {

            return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault())
                    .format((Instant) start);
        }

        return start.toString();
    }
}