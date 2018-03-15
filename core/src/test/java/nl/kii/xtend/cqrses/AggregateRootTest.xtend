package nl.kii.xtend.cqrses

import java.util.UUID
import org.junit.Test
import org.eclipse.xtend.lib.annotations.Data
import org.junit.Assert

class AggregateRootTest {

    @Test
    def void run() {
        val id  = UUID.randomUUID
        val demo = new Demo(id, 'Tester')
        Assert.assertEquals(id, demo.id)
        Assert.assertEquals('Tester', demo.text)
    }

    static class Demo extends AggregateRoot {
        var String text
        new(UUID id, String text) {
            apply(new DemoAdded(id,text))
        }
        def private void handle(DemoAdded event) {
            this.id = event.id
            this.text = event.text
        }
    }
    
    @Data
    static class DemoAdded implements Event {
        val UUID id
        val String text
    }
}
