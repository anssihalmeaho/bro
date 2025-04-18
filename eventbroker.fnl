
ns eventbroker

run = proc(mb own-box-name)
	import stddbc
	import stdvar
	import stdfu

	generate-id = proc()
		import stdstr
		import stdos
		import stdbytes

		ok err out errout = call(stdos.exec 'uuidgen'):
		call(stddbc.assert ok sprintf('%v (%v)' err errout))
		call(stdstr.strip call(stdbytes.string out))
	end
	own-instance-id = call(generate-id)

	create-mbox = get(mb 'create-mbox')
	sendmsg = get(mb 'sendmsg')
	recmsg = get(mb 'recmsg')
	register-with-name = get(mb 'register-with-name')
	id-by-name = get(mb 'id-by-name')
	contents = get(mb 'contents')
	all-names = get(mb 'all-names')

	subscriptions = call(stdvar.new list()) # list of pairs: topic/subsc-mbox-id

	del-subscriber = proc(subsc-box-id)
		del-ok del-err _ = call(stdvar.change
			subscriptions
			func(prev-subs)
				call(stdfu.filter prev-subs func(pair)
					not(eq(subsc-box-id last(pair)))
				end)
			end
		):
		list(del-ok del-err)
	end

	add-subscriber = proc(topic subsc-box-id)
		add-ok add-err _ = call(stdvar.change
			subscriptions
			func(prev-subs)
				if(in(prev-subs list(topic subsc-box-id))
					prev-subs
					append(prev-subs list(topic subsc-box-id))
				)
			end
		):
		list(add-ok add-err)
	end

	make-matcher = func(topic)
		import list-matcher
		func(pair)
			call(list-matcher.match head(pair) topic)
		end
	end

	publish = proc(topic message)
		all-subs = call(stdvar.value subscriptions)
		matched = call(stdfu.filter all-subs call(make-matcher topic))
		call(stdfu.proc-apply matched proc(pair)
			match-topic target-box = pair:
			call(sendmsg target-box list(topic message))
		end)
	end

	receiver = proc(rbox)
		while(true
			call(proc()
				_ request = call(recmsg rbox):
				case(head(request)
					'subs'
					call(proc()
						topic subsc-box-id subs-reply-box = rest(request):
						add-ok add-err = call(add-subscriber topic subsc-box-id):
						call(sendmsg subs-reply-box list('subs-ack' add-ok add-err topic own-instance-id))
					end)

					'unsub'
					call(proc()
						subsc-box-id subs-reply-box = rest(request):
						del-ok del-err = call(del-subscriber subsc-box-id):
						call(sendmsg subs-reply-box list('unsub-ack' del-ok del-err subsc-box-id own-instance-id))
					end)

					'pub'
					call(proc()
						#TODO: should there be reply box (pub ack) ?
						topic message = rest(request):
						call(publish topic message)
					end)

					'id'
					call(proc()
						reply-box = rest(request):
						call(sendmsg reply-box list('id-ack' own-instance-id))
					end)
				)
				rbox
			end)
			'whatever'
		)
	end

	ok err in-box = call(create-mbox 100):
	call(stddbc.assert ok err)
	call(register-with-name in-box own-box-name)

	call(receiver in-box)
end

endns

