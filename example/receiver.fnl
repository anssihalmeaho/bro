
ns main

import mbox
import bro
import sure

receiving = proc(mb)
	create-mbox = get(mb 'create-mbox')
	recmsg = get(mb 'recmsg')

	client = call(bro.new mb 'bro-box')
	subscribe = get(client 'subscribe')
	unsubscribe = get(client 'unsubscribe')

	box = call(sure.ok call(create-mbox 10))
	print('subscribe: ' call(subscribe list('...') box))

	print('...receiving...')
	call(proc()
		while(true
			call(proc()
				_ event = call(recmsg box):
				topic event-data = event:
				print('received event: ' event-data)
				if(eq(event-data 'cancel')
					print('unsubscribe: ' call(unsubscribe box))
					'none'
				)
				box
			end)
			'whatever'
		)
	end)
end

main = proc()
	mb = call(mbox.new
		'bro'
		'localhost:9903'
		list('localhost:9901' 'localhost:9902')
	)
	call(receiving mb)
end

endns

