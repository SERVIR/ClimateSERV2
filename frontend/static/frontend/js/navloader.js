function setActive(which) {
    if(which) {
        if ($("#" + which).length) {
            $("#" + which).addClass("active");
        } else {
            setTimeout(function () {
                setActive(which)
            }, 200);
        }
    }
}