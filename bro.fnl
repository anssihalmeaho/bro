
ns bro

new = proc(mb broker-box-name)
	import sure
	import stddbc
	import stdvar
	import stdfu
	import stdpr

	# set debug print functions
	debug = call(stdpr.get-pr false)
	debugpp = call(stdpr.get-pp-pr false)

	create-mbox = get(mb 'create-mbox')
	sendmsg = get(mb 'sendmsg')
	recmsg = get(mb 'recmsg')
	recmsg-with = get(mb 'recmsg-with')
	id-by-name = get(mb 'id-by-name')

	own-box = call(sure.ok call(create-mbox 10))
	subsc-ack-box = call(sure.ok call(create-mbox 100))
	subsc-store = call(stdvar.new list()) # used only for auto-resubscribing case
	broker-instance = call(stdvar.new '')

	# store own subscriptions for possible automatic re-subscribe
	store-subsc = proc(topic-pattern receiver-box)
		# maybe no need to check if its already (broker should reject)
		call(stdvar.change subsc-store func(prev)
			if(in(prev list(topic-pattern receiver-box))
				prev
				append(prev list(topic-pattern receiver-box))
			)
		end)
	end

	# remove subscription from local cache
	del-subsc = proc(receiver-box)
		call(stdvar.change subsc-store func(prev)
			call(stdfu.filter prev func(pair)
				not(eq(receiver-box last(pair)))
			end)
		end)
	end

	# act according to matching event broker id
	check-and-handle-broker-id = proc(reply-val)
		msg-name broker-id = reply-val:
		call(stddbc.assert eq(msg-name 'id-ack') 'unknown reply')
		prev-id = call(stdvar.value broker-instance)
		if(eq(prev-id broker-id)
			'matches, no action'

			# id mismatch, resubscribe all
			call(proc()
				call(stdvar.set broker-instance broker-id)
				subs = call(stdvar.value subsc-store)
				call(stdfu.proc-apply subs proc(one-subsc)
					call(subscribe one-subsc:)
				end)
			end)
		)
	end

	# periodical supervisor fiber
	spawn(call(proc()
		import stdtime
		while(
			call(proc()
				call(stdtime.sleep 3)
				target-found target-box = call(id-by-name broker-box-name):
				if(target-found
					call(proc()
						call(sendmsg target-box list('id' own-box))
						any-data val = call(recmsg-with own-box map('wait' true 'limit-sec' 2)):
						if(any-data
							call(check-and-handle-broker-id val)
							call(debug 'no reply from broker' '')
						)
					end)
					call(debug 'no connection' '')
				)
				true
			end)
			'whatever'
		)
	end))

	#TODO: improve result list forming (no direct assert)

	# publish event
	publish = proc(topic event-data)
		target-found target-box = call(id-by-name broker-box-name):
		call(stddbc.assert target-found 'broker box not found')
		call(sendmsg target-box list('pub' topic event-data))
	end

	# unsubscribe all events of given mbox
	unsubscribe = proc(receiver-box)
		#TODO: retrying unsubscribe on background if broker not reached ?
		#      (but then subscriptions and unsubscriptions should be kept consistent)
		#      (or separate synschronization with broker)
		ok err _ = tryl(call(proc()
			call(del-subsc receiver-box)

			target-found target-box = call(id-by-name broker-box-name):
			message = list(
				'unsub'
				receiver-box
				subsc-ack-box
			)
			call(sendmsg target-box message)
			call(recmsg-with subsc-ack-box map('wait' true 'limit-sec' 4))
			'no data'
		end)):
		list(ok err)
	end

	# subscribe events
	subscribe = proc(topic-pattern receiver-box)
		ok err _ = tryl(call(proc()
			# store subscription anyway, no error given if
			# event broker not reached
			call(store-subsc topic-pattern receiver-box)

			target-found target-box = call(id-by-name broker-box-name):
			#call(stddbc.assert target-found 'broker box not found')
			message = list(
				'subs'
				topic-pattern
				receiver-box
				subsc-ack-box
			)
			call(sendmsg target-box message)
			any-data val = call(recmsg-with subsc-ack-box map('wait' true 'limit-sec' 4)):
			#call(stddbc.assert any-data 'no subscribe ack')
			#call(stddbc.assert eq(head(val) 'subs-ack') sprintf('unexpected reply: ' val))
			'no data'
		end)):
		list(ok err)
	end

	# client object
	map(
		'subscribe'   subscribe
		'unsubscribe' unsubscribe
		'publish'     publish
	)
end

endns
