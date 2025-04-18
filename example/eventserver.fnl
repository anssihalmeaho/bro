
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
	print('subscribe: ' call(subscribe list('some' '*' '*' 'topic') box))

	print('...receiving...')
	call(proc()
		while(true
			call(proc()
				_ event = call(recmsg box):
				topic event-data = event:
				print('local event receive: ' event-data)
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
	import eventbroker

	mb = call(mbox.new
		'bro'
		'localhost:9901'
		list('localhost:9902' 'localhost:9903')
	)
	spawn(call(receiving mb))

	print('...eventserver running...')
	call(eventbroker.run mb 'bro-box')
end

endns

