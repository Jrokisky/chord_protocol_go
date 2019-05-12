$(document).ready(function() {
	$("#add-node-button").click(function(e) {
		$.ajax("http://localhost:8080/nodes", {"method":"POST"})
	});

	setInterval(function() {
		$.get("http://localhost:8080/nodes", function(data) {
			var nodes = JSON.parse(data);
			drawNodesTable(nodes);
			drawNodesChart(nodes);
			$(".join-link").click(function(e) {
				e.preventDefault();
				$.post($(this)[0].href);
			});
		})
	}, 1000);
});

function drawNodesChart(nodes) {
	var c = document.getElementById("chart-canvas");

	var width = c.width;
	var height = c.height;

	var center_x = (width/2);
	var center_y = (height/2);
	var min = Math.min(center_x, center_y);
	var radius = min * .75;

	var ctx = c.getContext("2d");
	ctx.clearRect(0, 0, c.width, c.height);
	ctx.imageSmoothingEnabled = false;
	ctx.strokeStyle = 'black';	
	ctx.beginPath();
	ctx.arc(center_x, center_y, radius, -Math.PI/2, 3 * Math.PI / 2);
	ctx.stroke();

	var keys = Object.keys(nodes)
	for (var i = 0; i < keys.length; i++) {
		var node = nodes[keys[i]];
		var max_id = 4294967295;
		var ratio = node.ID / max_id;
		var radians = (ratio * 2 * Math.PI) - (Math.PI/2);
		var y = Math.sin(radians) * radius;
		var x = Math.cos(radians) * radius;

		var ctx = c.getContext("2d");
		ctx.beginPath();
		ctx.arc(center_x + x, center_y + y, 3, 0, 2 * Math.PI);
		if (node.InRing) {
			ctx.strokeStyle = 'green';	
		}
		else {
			ctx.strokeStyle = 'red';	
		}
		ctx.stroke();

		ctx.font = "4 px Comic Sans MS";
		ctx.fillStyle = "black";
		ctx.textAlign = "center";
		ctx.fillText(i, center_x + x + 8, center_y + y + 8); 	

		// Draw connecting line.
		if (node.Successor && node.Successor != 0) {
			var ratio = node.Successor / max_id;
			var radians = (ratio * 2 * Math.PI) - (Math.PI/2);
			var sy = Math.sin(radians) * radius;
			var sx = Math.cos(radians) * radius;

			ctx.strokeStyle = 'blue';	
			ctx.beginPath();
			ctx.moveTo(center_x + x, center_y + y);
			ctx.lineTo(center_x + sx, center_y + sy);
			ctx.stroke();
		}
	}
}

function drawNodesTable(nodes) {
	var $nodeList = $("#node-list");
	$nodeList.empty();
	var table = document.createElement('table')
	var keys = Object.keys(nodes)

	// Header.
	var tr = document.createElement('tr');   
	var idx = document.createTextNode("IDX");
    	var idx_th = document.createElement('th');
	idx_th.appendChild(idx);
    	tr.appendChild(idx_th);

	var id = document.createTextNode("ID");
    	var id_th = document.createElement('th');
	id_th.appendChild(id);
    	tr.appendChild(id_th);

	var succ = document.createTextNode("SUCC");
    	var succ_th = document.createElement('th');
	succ_th.appendChild(succ);
    	tr.appendChild(succ_th);

	var pred = document.createTextNode("PRED");
    	var pred_th = document.createElement('th');
	pred_th.appendChild(pred);
    	tr.appendChild(pred_th);

	var join_a = document.createTextNode("OPERATION");
    	var join_a_th = document.createElement('th');
	join_a_th.appendChild(join_a);
    	tr.appendChild(join_a_th);
	table.appendChild(tr);
	// End Header.

	for (var i = 0; i < keys.length; i++) {
		var node = nodes[keys[i]];
		tr = document.createElement('tr');   

		var idx = document.createTextNode(i);
    		var idx_td = document.createElement('td');
    		idx_td.appendChild(idx);
    		tr.appendChild(idx_td);

		var id = document.createTextNode(node.ID);
    		var id_td = document.createElement('td');
    		id_td.appendChild(id);
    		tr.appendChild(id_td);

    		var succ = document.createTextNode(node.Successor);
    		var succ_td = document.createElement('td');
    		succ_td.appendChild(succ);
    		tr.appendChild(succ_td);

    		var pred = document.createTextNode(node.Predecessor);
    		var pred_td = document.createElement('td');
    		pred_td.appendChild(pred);
    		tr.appendChild(pred_td);

		if (!node.InRing) {
    			var join_link = document.createElement('a');
			var join_link_text = document.createTextNode("Join");
			join_link.appendChild(join_link_text);
			join_link.title = "Join";
			join_link.href = "http://localhost:8080/nodes/" + node.ID + "/join";
			join_link.className = "join-link";

    			var join_link_td = document.createElement('td');
    			join_link_td.appendChild(join_link);
    			tr.appendChild(join_link_td);
		}

		table.appendChild(tr);
	}
	$nodeList[0].appendChild(table)
}
