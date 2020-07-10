import com.xh.kudu.Thread.HiveColumnsThread;
import com.xh.kudu.Thread.MySqlColumnsThread;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;

public class ThreadTest {




    @Test
    public void testThreadSynchronise() throws InterruptedException {
//        Thread thread1 = new Thread(new MySqlColumnsThread());
//        thread1.start();
//        thread1.join();
//
//        Thread thread2 = new Thread(new HiveColumnsThread());
//        thread2.start();
//        thread2.join();
//        System.out.println("end");

        System.out.println(CollectionUtils.isEqualCollection(null,null));

    }
}
