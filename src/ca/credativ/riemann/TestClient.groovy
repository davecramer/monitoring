package ca.credativ.riemann

import com.aphyr.riemann.Proto
import com.aphyr.riemann.client.RiemannClient
import groovy.sql.Sql

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

/**
 * Created with IntelliJ IDEA.
 * User: davec
 * Date: 13-10-07
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
class TestClient
{
  public static void main(String []args)
  {
    RiemannClient riemannClient = RiemannClient.udp("monitor.xtuple.com", 5555);

    Sql sql
    def select = """select numbackends as connections, xact_commit as transactions, xact_rollback as rollback,
                    blks_read as BlocksRead, blks_hit as BlocksHit, tup_returned as TuplesReturned,
                    tup_fetched as TuplesFetched, tup_inserted as TuplesInserted, tup_updated as TuplesUpdated,
                    tup_deleted as TuplesDeleted, conflicts, stats_reset as reset from pg_stat_database
                    where datname = :db
                """

    riemannClient.every(5000,TimeUnit.MILLISECONDS,new Runnable() {
        public void run()
        {
            try {
               if ( !riemannClient.connected )
               {
                   riemannClient.connect()
               }
               sql = Sql.newInstance('jdbc:postgresql://db01-mobile.xtuple.com/template1',
                               'monitor','ilatectu','org.postgresql.Driver')

               List <Proto.Event> events = []
               sql.eachRow(select, [db:'dogfood']) { row ->

                   // don't send the reset time so send all data but the last column
                   (1..row.getMetaData().columnCount-1).each { columnOrdinal ->

                      def metric = row.getMetaData().getColumnName(columnOrdinal)
                      Proto.Event event = riemannClient.event().
                              host("com.xtuple.dogfood").
                              service(metric).
                              metric(row.getProperty(metric)).
                              build()
                      events << event


                   }
                   riemannClient.sendEvents(events)

               }
               sql.close()
               riemannClient.disconnect()
            }
            catch ( Throwable th )
            {
               th.printStackTrace(System.err)
            }
        }
    }  )
  }
}
