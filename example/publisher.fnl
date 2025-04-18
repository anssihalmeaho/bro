
ns main

import mbox
import bro

main = proc(content)
	mb = call(mbox.new
		'bro'
		'localhost:9902'
		list('localhost:9901' 'localhost:9903')
	)
	client = call(bro.new mb 'bro-box')
	publish = get(client 'publish')

	print('pub: 'call(publish
		list('some' 'kind' 'of' 'topic')
		content
	))
end

endns

