Pilar module for *pyobs*
========================


Install *pyobs-pilar*
-------------------
Clone the repository:

    git clone https://github.com/pyobs/pyobs-pilar.git


Install dependencies:

    cd pyobs-pilar
    pip3 install -r requirements
        
And install it:

    python3 setup.py install


Configuration
-------------
The *FliCamera* class is derived from *BaseCamera* (see *pyobs* documentation) and adds a single new parameter:

    setpoint:
        The initial setpoint in degrees Celsius for the cooling of the camera.

The class works fine with its default parameters, so a basic module configuration would look like this:

    module:
      class: pyobs_fli.FliCamera
      name: FLI camera

Dependencies
------------
* **pyobs** for the core funcionality. It is not included in the *requirements.txt*, so needs to be installed 
  separately.
* [Astropy](http://www.astropy.org/) for FITS file handling.
