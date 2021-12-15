let bob;
let map;
let uploadLayer;

function showAOI(which){
    let aoi;

    bob = which;
    try{
        aoi = JSON.parse(which.innerText);

        uploadLayer.clearLayers();
        uploadLayer.addData(aoi);

        try {
                    map.fitBounds(uploadLayer.getBounds());
                } catch(e) {
            map.fitBounds([
                [data.bbox[1], data.bbox[0]],
                [data.bbox[3], data.bbox[2]],
            ]);
        }
    } catch(e){
        try {

        } catch (e){

        }
    }
    console.log(aoi);
}

function deleteRow(which){
    console.log("Delete: " + which);
}

function initMap() {
    map = L.map("map", {
        zoom: 3,
        fullscreenControl: true,
        center: [38.0, 15.0],
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);
    uploadLayer = L.geoJson().addTo(map);
}

$(function(){
    initMap();
});