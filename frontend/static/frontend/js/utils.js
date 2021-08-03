getParameterByName = (name, url) => {
    const regex = new RegExp(
        "[?&]" + name.replace(/[[\]]/g, "\\$&") + "(=([^&#]*)|&|#|$)"
    );
    const results = regex.exec(decodeURIComponent(url || window.location.href));
    return results
        ? results[2]
            ? decodeURIComponent(results[2].replace(/\+/g, " "))
            : ""
        : null;
};