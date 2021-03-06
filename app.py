#!/usr/bin/env python

from multiprocessing import Process, Queue

from worker import workloop
from os import environ


optionsDict = {}


def options():
    global optionsDict
    return optionsDict


def model_collection():
    dburl = options()["db_url"]
    return pymongo.MongoClient(dburl).mapdemo.summaries


def query_collection():
    dburl = options()["db_url"]
    return pymongo.MongoClient(dburl).mapdemo.queries


def static_speeds():
    """ Hardcoded polyline for testing """
    return '[{"color": "#b2abd2", "points": [[42.336056, -71.15166], [42.333887, -71.154243]]}, {"color": "#f7f7f7", "points": [[42.333804, -71.154278], [42.332562, -71.159012]]}, {"color": "#f7f7f7", "points": [[42.332636, -71.159089], [42.333804, -71.16518]]}, {"color": "#b2abd2", "points": [[42.333791, -71.16529], [42.333134, -71.170881]]}, {"color": "#b2abd2", "points": [[42.333121, -71.170974], [42.332906, -71.172572]]}, {"color": "#f7f7f7", "points": [[42.332906, -71.172572], [42.332144, -71.17705]]}, {"color": "#fdb863", "points": [[42.332163, -71.177196], [42.331894, -71.18321]]}, {"color": "#fdb863", "points": [[42.331872, -71.183351], [42.330669, -71.191223]]}, {"color": "#b2abd2", "points": [[42.330654, -71.191315], [42.33035, -71.197013]]}, {"color": "#f7f7f7", "points": [[42.330363, -71.197123], [42.33067, -71.203783]]}, {"color": "#f7f7f7", "points": [[42.330678, -71.203892], [42.331134, -71.210583]]}, {"color": "#f7f7f7", "points": [[42.331141, -71.210705], [42.330832, -71.217596]]}, {"color": "#f7f7f7", "points": [[42.330819, -71.217707], [42.330184, -71.221347]]}, {"color": "#f7f7f7", "points": [[42.330162, -71.221449], [42.328855, -71.227002]]}, {"color": "#f7f7f7", "points": [[42.328811, -71.227098], [42.326048, -71.231733]]}, {"color": "#f7f7f7", "points": [[42.326033, -71.231781], [42.327116, -71.236773]]}, {"color": "#f7f7f7", "points": [[42.32714, -71.236896], [42.328754, -71.24298]]}, {"color": "#f7f7f7", "points": [[42.328787, -71.243077], [42.329782, -71.249702]]}, {"color": "#f7f7f7", "points": [[42.329735, -71.249832], [42.327818, -71.252833]]}, {"color": "#f7f7f7", "points": [[42.327818, -71.252833], [42.32556, -71.257255]]}, {"color": "#f7f7f7", "points": [[42.325543, -71.257326], [42.328083, -71.262243]]}, {"color": "#f7f7f7", "points": [[42.32812, -71.26237], [42.33089, -71.268049]]}, {"color": "#f7f7f7", "points": [[42.330934, -71.26807], [42.330988, -71.273219]]}, {"color": "#b2abd2", "points": [[42.330932, -71.273276], [42.330759, -71.276355]]}, {"color": "#f7f7f7", "points": [[42.330774, -71.276409], [42.328151, -71.27991]]}, {"color": "#f7f7f7", "points": [[42.328051, -71.279904], [42.325606, -71.277668]]}, {"color": "#b2abd2", "points": [[42.325606, -71.277668], [42.325376, -71.278587]]}, {"color": "#f7f7f7", "points": [[42.325361, -71.278694], [42.325733, -71.283732]]}, {"color": "#f7f7f7", "points": [[42.325754, -71.283807], [42.32718, -71.289298]]}, {"color": "#b2abd2", "points": [[42.327207, -71.2894], [42.328415, -71.293332]]}, {"color": "#fdb863", "points": [[42.328435, -71.293398], [42.328722, -71.300068]]}, {"color": "#fdb863", "points": [[42.328767, -71.300191], [42.331515, -71.306582]]}, {"color": "#f7f7f7", "points": [[42.3315, -71.306676], [42.331485, -71.307075]]}, {"color": "#f7f7f7", "points": [[42.331485, -71.307075], [42.327358, -71.309815]]}, {"color": "#f7f7f7", "points": [[42.327262, -71.30985], [42.32535, -71.315323]]}, {"color": "#e66101", "points": [[42.325302, -71.315425], [42.325265, -71.321273]]}, {"color": "#f7f7f7", "points": [[42.325285, -71.321352], [42.326834, -71.321352]]}, {"color": "#f7f7f7", "points": [[42.326901, -71.321362], [42.330355, -71.316673]]}, {"color": "#fdb863", "points": [[42.330387, -71.316556], [42.334812, -71.314254]]}, {"color": "#f7f7f7", "points": [[42.334881, -71.314248], [42.336361, -71.311615]]}, {"color": "#f7f7f7", "points": [[42.336352, -71.311498], [42.336086, -71.307532]]}, {"color": "#f7f7f7", "points": [[42.336086, -71.307532], [42.33931, -71.306653]]}, {"color": "#f7f7f7", "points": [[42.339385, -71.30662], [42.344104, -71.305477]]}, {"color": "#f7f7f7", "points": [[42.344189, -71.305468], [42.348171, -71.30623]]}, {"color": "#e66101", "points": [[42.348247, -71.306242], [42.354438, -71.302665]]}, {"color": "#fdb863", "points": [[42.354539, -71.302576], [42.358379, -71.297166]]}, {"color": "#f7f7f7", "points": [[42.358432, -71.297049], [42.359164, -71.294418]]}, {"color": "#b2abd2", "points": [[42.359164, -71.294418], [42.356905, -71.293273]]}, {"color": "#b2abd2", "points": [[42.356846, -71.293215], [42.353633, -71.289319]]}, {"color": "#fdb863", "points": [[42.35359, -71.289228], [42.349818, -71.282856]]}, {"color": "#fdb863", "points": [[42.349721, -71.282786], [42.343997, -71.280606]]}, {"color": "#e66101", "points": [[42.34392, -71.28049], [42.341199, -71.273092]]}, {"color": "#b2abd2", "points": [[42.341191, -71.273076], [42.340886, -71.27187]]}, {"color": "#f7f7f7", "points": [[42.340892, -71.27179], [42.337618, -71.267221]]}, {"color": "#fdb863", "points": [[42.337501, -71.267239], [42.331293, -71.268479]]}, {"color": "#b2abd2", "points": [[42.331269, -71.26855], [42.330929, -71.273323]]}, {"color": "#b2abd2", "points": [[42.330892, -71.273369], [42.33053, -71.275859]]}, {"color": "#b2abd2", "points": [[42.330551, -71.275917], [42.329609, -71.279264]]}, {"color": "#f7f7f7", "points": [[42.329556, -71.279317], [42.325756, -71.27781]]}, {"color": "#5e3c99", "points": [[42.325742, -71.277801], [42.325712, -71.277784]]}, {"color": "#fdb863", "points": [[42.325712, -71.277784], [42.323542, -71.271354]]}, {"color": "#e66101", "points": [[42.323527, -71.271194], [42.323724, -71.262403]]}, {"color": "#f7f7f7", "points": [[42.323713, -71.262333], [42.325729, -71.256373]]}, {"color": "#f7f7f7", "points": [[42.325754, -71.256294], [42.326638, -71.252218]]}, {"color": "#f7f7f7", "points": [[42.326576, -71.252113], [42.323587, -71.246996]]}, {"color": "#b2abd2", "points": [[42.323576, -71.246915], [42.323821, -71.243906]]}, {"color": "#f7f7f7", "points": [[42.323806, -71.24385], [42.323899, -71.238432]]}, {"color": "#f7f7f7", "points": [[42.323913, -71.23833], [42.325121, -71.232101]]}, {"color": "#f7f7f7", "points": [[42.325185, -71.232033], [42.32831, -71.227947]]}, {"color": "#f7f7f7", "points": [[42.328358, -71.227851], [42.329508, -71.223591]]}, {"color": "#fdb863", "points": [[42.329533, -71.223487], [42.330848, -71.215702]]}, {"color": "#f7f7f7", "points": [[42.330858, -71.215568], [42.330943, -71.208535]]}, {"color": "#b2abd2", "points": [[42.330939, -71.20845], [42.330592, -71.203189]]}, {"color": "#f7f7f7", "points": [[42.330589, -71.203103], [42.330269, -71.196655]]}, {"color": "#b2abd2", "points": [[42.330257, -71.196557], [42.330048, -71.194958]]}, {"color": "#b2abd2", "points": [[42.330052, -71.194882], [42.330345, -71.193047]]}, {"color": "#f7f7f7", "points": [[42.330345, -71.193047], [42.331313, -71.186749]]}, {"color": "#f7f7f7", "points": [[42.331336, -71.18661], [42.332065, -71.18149]]}, {"color": "#f7f7f7", "points": [[42.332079, -71.181403], [42.33225, -71.175927]]}, {"color": "#b2abd2", "points": [[42.332275, -71.175863], [42.332773, -71.173201]]}, {"color": "#fdb863", "points": [[42.332773, -71.173201], [42.333479, -71.167073]]}, {"color": "#fdb863", "points": [[42.333498, -71.166917], [42.332726, -71.159302]]}, {"color": "#f7f7f7", "points": [[42.332651, -71.159211], [42.334065, -71.153983]]}, {"color": "#f7f7f7", "points": [[42.334155, -71.153941], [42.335886, -71.151076]]}, {"color": "#b2abd2", "points": [[42.335888, -71.151057], [42.336745, -71.146158]]}, {"color": "#b2abd2", "points": [[42.336767, -71.14606], [42.33781, -71.141012]]}, {"color": "#b2abd2", "points": [[42.337821, -71.140943], [42.338766, -71.13708]]}, {"color": "#f7f7f7", "points": [[42.338789, -71.137002], [42.339559, -71.13008]]}, {"color": "#f7f7f7", "points": [[42.339577, -71.129946], [42.340442, -71.126839]]}, {"color": "#f7f7f7", "points": [[42.340442, -71.126839], [42.341676, -71.122241]]}, {"color": "#f7f7f7", "points": [[42.341704, -71.122137], [42.342456, -71.119462]]}, {"color": "#b2abd2", "points": [[42.34248, -71.119371], [42.343749, -71.114804]]}, {"color": "#b2abd2", "points": [[42.343764, -71.114745], [42.344469, -71.112193]]}, {"color": "#b2abd2", "points": [[42.344496, -71.112086], [42.345325, -71.10911]]}, {"color": "#b2abd2", "points": [[42.345327, -71.10908], [42.346467, -71.105236]]}, {"color": "#5e3c99", "points": [[42.34647, -71.105225], [42.346998, -71.103598]]}, {"color": "#b2abd2", "points": [[42.347025, -71.103515], [42.348223, -71.09911]]}, {"color": "#b2abd2", "points": [[42.348243, -71.099021], [42.348764, -71.094401]]}, {"color": "#b2abd2", "points": [[42.348773, -71.094299], [42.348849, -71.089743]]}, {"color": "#b2abd2", "points": [[42.348859, -71.089636], [42.349492, -71.086148]]}, {"color": "#b2abd2", "points": [[42.349492, -71.086148], [42.349268, -71.084281]]}, {"color": "#5e3c99", "points": [[42.349267, -71.084281], [42.348359, -71.083508]]}, {"color": "#5e3c99", "points": [[42.348374, -71.083446], [42.349169, -71.080813]]}, {"color": "#b2abd2", "points": [[42.349162, -71.080793], [42.347108, -71.079804]]}, {"color": "#f7f7f7", "points": [[42.347107, -71.079802], [42.345895, -71.083055]]}, {"color": "#5e3c99", "points": [[42.345901, -71.08313], [42.346308, -71.084309]]}]'

    
from flask import Flask, render_template, url_for, request, make_response, redirect
from flask.json import jsonify

import pymongo
import json

from uuid import uuid4, UUID

app = Flask(__name__)

if __name__ == '__main__':
    train_q = Queue()
    result_q = Queue()

    master = environ.get("MAP_MASTER", "local[*]")
    dburl = environ.get("MAP_DBURL", "mongodb://localhost")

    options()["spark_master"] = master
    options()["db_url"] = dburl
    options()["train_queue"] = train_q
    options()["result_queue"] = result_q

    p = Process(target=workloop, args=(master, train_q, result_q, dburl))
    p.start()

    # wait for worker to spin up
    result_q.get()

    def sanitize_model(m):
        result = dict([(k, m[k]) for k in ["_id", "name", "urls", "status"]])
        result["id"] = result.pop("_id")
        return result
    
    @app.route("/live")
    def live():
        response = None
        try:
            model_collection().find()
            response = make_response("all is well!", 200)
        except Exception, e:
            response = make_response("not ready yet", 400)
        return response
    
    @app.route("/")
    def hello():
        return "hello, world!"
    
    @app.route("/summaries", methods=['GET', 'POST'])
    def create_or_list_summaries():
        if request.method == 'GET':
            dburl = options()["db_url"]
            results = pymongo.MongoClient(dburl).mapdemo.summaries.find()
            if request.args.has_key("json"):
                return jsonify([{"name": s["name"], "id": s["_id"], "status": s["status"]} for s in results])
            else:
                postprocessed = [{"name": s["name"], "status": s["status"], "mapurl": url_for("render_map", mapid=s["_id"]), "summaryurl": url_for("get_summary", mapid=s["_id"])} for s in results]
                return render_template("summaries.html", results=postprocessed, summaries_url=url_for("create_or_list_summaries"))
        else:
            job = {"url": request.form['url'], "_id": uuid4(),
                   "name": request.form['name'], "status": "training"}
            (model_collection()).insert_one(job)
            options()["train_queue"].put(job)
            return redirect(url_for("create_or_list_summaries"), code=302)

    @app.route("/summaries/<mapid>", methods=['GET'])
    def get_summary(mapid):
        model = model_collection().find_one({"_id": UUID(mapid)})
        response = make_response("timestamp,startlat,startlon,endlat,endlon,maxmph\n" + "\n".join([",".join([str(r) for r in row]) for row in model["summary"]]))
        response.mimetype = "text/plain"
        return response
    
    @app.route("/maps/<mapid>")
    def render_map(mapid):
        if mapid == "glv":
            return render_template("map.html", speeds=static_speeds())

        model = model_collection().find_one({"_id": UUID(mapid)})
        if model is None:
            return json_error("Not Found", 404, "can't find summary with ID %r" % mapid, 404)
        return render_template("map.html", speeds=json.dumps(model["polyline"]))

    app.run(port=8080, host='0.0.0.0')
