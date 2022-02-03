/**
 * Sets the active “you can do” slide
 * @param {int} which - index of the slide to activate
 */
function setYouCanDoSlide(which) {
  $("#usageCarousel").carousel(which);
}

function decodeHtml(html) {
    const txt = document.createElement("textarea");
    txt.innerHTML = html;
    return txt.value;
}

/**
 * Sets up the carousels and loads data into the teaser from the dataItems structure.
 * It also creates cloned copies of the slides and mashes them into new slides that
 * contain three slides each in order to have the three slides showing.
 */
function initIndex() {
  $("#main-carousel").carousel();
  $("#usageCarousel").carousel({
    interval: false,
  });

  $("#dataCarousel").carousel({
    interval: 5000,
    transition: "transform 4s ease, opacity 2s ease-out"
  });

  preloadCarousel();
}

/**
 * Preloads the carousel images
 */
function preloadCarousel() {
  $('body').append('<div class="preload" aria-hidden="true" style="position: absolute; left: -9999px; height: 1px; width: 1px; overflow: hidden;"></div>');
  const image_tags = [];
  $('.carousel .carousel-item').each(function() {
    const this_image = $(this).css('background-image').replace(/^url\(['"](.+)['"]\)/, '$1');
    if(this_image != "none") {
      const img_tag = '<img src="' + this_image + '" alt="" /><br>';
      image_tags.push(img_tag);
    }
  });
  $('.preload').append(image_tags);
}


/**
 * Calls initIndex on ready
 *
 * @event index-ready
 */
$(function () {
  initIndex();
});
