let bob;

function showAOI(which){
    let aoi;

    bob = which;
    try{
        aoi = JSON.parse(which.innerText);
    } catch(e){
        try {

        } catch (e){

        }
    }
    console.log(aoi);
}